package convert

import (
	"database/sql"
	"fmt"
	"log/slog"
	"strings"
	"time"
)

type scanner interface {
	Scan(dest ...any) error
}

const getRecordsTmpl = `
	SELECT hardlinks.id, localPath, inode, localFiles.lastUploaded FROM localFiles
	JOIN remoteFiles ON localFiles.remoteFileID = remoteFiles.id
	JOIN hardlinks ON remoteFiles.hardlinkID = hardlinks.id
	WHERE localPath IN (%s)
`

const setInodeStmt = "UPDATE hardlinks SET inode = ? WHERE id = ?"

type DBHelper struct {
	db *sql.DB
}

func NewDBHelper(driver, url string) (*DBHelper, error) {
	connection, err := sql.Open(driver, url)
	if err != nil {
		return nil, err
	}

	return &DBHelper{db: connection}, nil
}

func (db *DBHelper) Close() error {
	return db.db.Close()
}

type Record struct {
	LocalPath  string
	Inode      uint64
	Uploaded   time.Time
	HardlinkID uint64
}

func (db *DBHelper) GetRecords(localPaths []string) ([]*Record, error) {
	args := make([]interface{}, len(localPaths))
	for i, path := range localPaths {
		args[i] = path
	}

	placeholders := strings.TrimRight(strings.Repeat("?,", len(localPaths)), ",")
	stmt := fmt.Sprintf(getRecordsTmpl, placeholders)

	rows, err := db.db.Query(stmt, args...)
	if err != nil {
		return nil, err
	}

	defer callAndLogError(rows.Close)

	var records []*Record

	for rows.Next() {
		record, err := scanRecord(rows)
		if err != nil {
			return nil, err
		}

		records = append(records, record)
	}

	return records, nil
}

func callAndLogError(f func() error) {
	err := f()
	if err != nil {
		slog.Error(err.Error())
	}
}

func scanRecord(row scanner) (*Record, error) {
	var record Record

	err := row.Scan(&record.HardlinkID, &record.LocalPath, &record.Inode, &record.Uploaded)

	if err != nil {
		return nil, err
	}

	return &record, nil
}

func (db *DBHelper) ReplaceInode(hardlinkID uint64, inode uint64) error {
	_, err := db.db.Exec(setInodeStmt, inode, hardlinkID)
	return err
}
