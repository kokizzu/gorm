package gorm

import (
	"crypto/sha1"
	"database/sql"
	"fmt"
	"reflect"
	"regexp"
	"strings"
	"time"
	"unicode/utf8"
)

var memsqlIndexRegex = regexp.MustCompile(`^(.+)\((\d+)\)$`)

type memsql struct {
	commonDialect
}

func init() {
	RegisterDialect("memsql", &memsql{})
}

func (memsql) GetName() string {
	return "memsql"
}

func (memsql) Quote(key string) string {
	return fmt.Sprintf("`%s`", key)
}

// Get Data Type for Memsql Dialect
func (s *memsql) DataTypeOf(field *StructField) string {
	var dataValue, sqlType, size, additionalType = ParseFieldStructForDialect(field, s)

	// Memsql allows only one auto increment column per table, and it must
	// be a KEY column.
	if _, ok := field.TagSettingsGet("AUTO_INCREMENT"); ok {
		if _, ok = field.TagSettingsGet("INDEX"); !ok && !field.IsPrimaryKey {
			field.TagSettingsDelete("AUTO_INCREMENT")
		}
	}

	if sqlType == "" {
		switch dataValue.Kind() {
		case reflect.Bool:
			sqlType = "boolean"
		case reflect.Int8:
			if s.fieldCanAutoIncrement(field) {
				field.TagSettingsSet("AUTO_INCREMENT", "AUTO_INCREMENT")
				sqlType = "tinyint AUTO_INCREMENT"
			} else {
				sqlType = "tinyint"
			}
		case reflect.Int, reflect.Int16, reflect.Int32:
			if s.fieldCanAutoIncrement(field) {
				field.TagSettingsSet("AUTO_INCREMENT", "AUTO_INCREMENT")
				sqlType = "int AUTO_INCREMENT"
			} else {
				sqlType = "int"
			}
		case reflect.Uint8:
			if s.fieldCanAutoIncrement(field) {
				field.TagSettingsSet("AUTO_INCREMENT", "AUTO_INCREMENT")
				sqlType = "tinyint unsigned AUTO_INCREMENT"
			} else {
				sqlType = "tinyint unsigned"
			}
		case reflect.Uint, reflect.Uint16, reflect.Uint32, reflect.Uintptr:
			if s.fieldCanAutoIncrement(field) {
				field.TagSettingsSet("AUTO_INCREMENT", "AUTO_INCREMENT")
				sqlType = "int unsigned AUTO_INCREMENT"
			} else {
				sqlType = "int unsigned"
			}
		case reflect.Int64:
			if s.fieldCanAutoIncrement(field) {
				field.TagSettingsSet("AUTO_INCREMENT", "AUTO_INCREMENT")
				sqlType = "bigint AUTO_INCREMENT"
			} else {
				sqlType = "bigint"
			}
		case reflect.Uint64:
			if s.fieldCanAutoIncrement(field) {
				field.TagSettingsSet("AUTO_INCREMENT", "AUTO_INCREMENT")
				sqlType = "bigint unsigned AUTO_INCREMENT"
			} else {
				sqlType = "bigint unsigned"
			}
		case reflect.Float32, reflect.Float64:
			sqlType = "double"
		case reflect.String:
			if size > 0 && size < 65532 {
				sqlType = fmt.Sprintf("varchar(%d)", size)
			} else {
				sqlType = "longtext"
			}
		case reflect.Struct:
			if _, ok := dataValue.Interface().(time.Time); ok {
				precision := ""
				if p, ok := field.TagSettingsGet("PRECISION"); ok {
					precision = fmt.Sprintf("(%s)", p)
				}

				if _, ok := field.TagSettings["NOT NULL"]; ok || field.IsPrimaryKey {
					sqlType = fmt.Sprintf("DATETIME%v", precision)
				} else {
					sqlType = fmt.Sprintf("DATETIME%v NULL", precision)
				}
			}
		default:
			if IsByteArrayOrSlice(dataValue) {
				if size > 0 && size < 65532 {
					sqlType = fmt.Sprintf("varbinary(%d)", size)
				} else {
					sqlType = "longblob"
				}
			}
		}
	}

	if sqlType == "" {
		panic(fmt.Sprintf("invalid sql type %s (%s) in field %s for memsql", dataValue.Type().Name(), dataValue.Kind().String(), field.Name))
	}

	if strings.TrimSpace(additionalType) == "" {
		return sqlType
	}
	return fmt.Sprintf("%v %v", sqlType, additionalType)
}

func (s memsql) RemoveIndex(tableName string, indexName string) error {
	_, err := s.db.Exec(fmt.Sprintf("DROP INDEX %v ON %v", indexName, s.Quote(tableName)))
	return err
}

func (s memsql) ModifyColumn(tableName string, columnName string, typ string) error {
	_, err := s.db.Exec(fmt.Sprintf("ALTER TABLE %v MODIFY COLUMN %v %v", tableName, columnName, typ))
	return err
}

func (s memsql) LimitAndOffsetSQL(limit, offset interface{}) (sql string, err error) {
	if limit != nil {
		parsedLimit, err := s.parseInt(limit)
		if err != nil {
			return "", err
		}
		if parsedLimit >= 0 {
			sql += fmt.Sprintf(" LIMIT %d", parsedLimit)

			if offset != nil {
				parsedOffset, err := s.parseInt(offset)
				if err != nil {
					return "", err
				}
				if parsedOffset >= 0 {
					sql += fmt.Sprintf(" OFFSET %d", parsedOffset)
				}
			}
		}
	}
	return
}

func (s memsql) HasForeignKey(tableName string, foreignKeyName string) bool {
	var count int
	currentDatabase, tableName := currentDatabaseAndTable(&s, tableName)
	s.db.QueryRow("SELECT count(*) FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS WHERE CONSTRAINT_SCHEMA=? AND TABLE_NAME=? AND CONSTRAINT_NAME=? AND CONSTRAINT_TYPE='FOREIGN KEY'", currentDatabase, tableName, foreignKeyName).Scan(&count)
	return count > 0
}

func (s memsql) HasTable(tableName string) bool {
	currentDatabase, tableName := currentDatabaseAndTable(&s, tableName)
	var name string
	// allow memsql database name with '-' character
	if err := s.db.QueryRow("SELECT table_name FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?", currentDatabase, tableName).Scan(&name); err != nil {
		if err == sql.ErrNoRows {
			return false
		}
		panic(err)
	} else {
		return true
	}
}

func (s memsql) HasIndex(tableName string, indexName string) bool {
	currentDatabase, tableName := currentDatabaseAndTable(&s, tableName)
	if rows, err := s.db.Query("SELECT index_name FROM INFORMATION_SCHEMA.INDEX_STATISTICS WHERE DATABASE_NAME = ? AND TABLE_NAME = ? AND INDEX_NAME = ?", currentDatabase, tableName, indexName); err != nil {
		panic(err)
	} else {
		defer rows.Close()
		return rows.Next()
	}
}

func (s memsql) HasColumn(tableName string, columnName string) bool {
	currentDatabase, tableName := currentDatabaseAndTable(&s, tableName)
	if rows, err := s.db.Query("SELECT column_name FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND COLUMN_NAME = ?", currentDatabase, tableName, columnName); err != nil {
		panic(err)
	} else {
		defer rows.Close()
		return rows.Next()
	}
}

func (s memsql) CurrentDatabase() (name string) {
	s.db.QueryRow("SELECT DATABASE()").Scan(&name)
	return
}

func (memsql) SelectFromDummyTable() string {
	return "FROM DUAL"
}

func (s memsql) BuildKeyName(kind, tableName string, fields ...string) string {
	keyName := s.commonDialect.BuildKeyName(kind, tableName, fields...)
	if utf8.RuneCountInString(keyName) <= 64 {
		return keyName
	}
	h := sha1.New()
	h.Write([]byte(keyName))
	bs := h.Sum(nil)

	// sha1 is 40 characters, keep first 24 characters of destination
	destRunes := []rune(keyNameRegex.ReplaceAllString(fields[0], "_"))
	if len(destRunes) > 24 {
		destRunes = destRunes[:24]
	}

	return fmt.Sprintf("%s%x", string(destRunes), bs)
}

// NormalizeIndexAndColumn returns index name and column name for specify an index prefix length if needed
func (memsql) NormalizeIndexAndColumn(indexName, columnName string) (string, string) {
	submatch := memsqlIndexRegex.FindStringSubmatch(indexName)
	if len(submatch) != 3 {
		return indexName, columnName
	}
	indexName = submatch[1]
	columnName = fmt.Sprintf("%s(%s)", columnName, submatch[2])
	return indexName, columnName
}

func (memsql) DefaultValueStr() string {
	return "VALUES()"
}
