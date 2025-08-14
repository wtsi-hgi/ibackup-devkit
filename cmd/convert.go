package cmd

import (
	"errors"
	"fmt"
	"github.com/spf13/cobra"
	"github.com/wtsi-hgi/ibackup/transfer"
	"log"
	"os"
	"strings"
	"time"

	"github.com/wtsi-hgi/ibackup/db"
	"github.com/wtsi-hgi/ibackup/set"
)

var ErrWrongTransformer = errors.New("wrong transformer")
var ErrNoSQLCredentials = errors.New("connection details for MySQL are not set")
var ErrWrongMetadata = errors.New("wrong metadata value for key")

func buildURL(host, port, dbName, user, password string) string {
	return fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?parseTime=true", user, password, host, port, dbName)
}

func BuildSQLURL() (string, error) {
	host := os.Getenv("MYSQL_HOST")
	port := os.Getenv("MYSQL_PORT")
	dbName := os.Getenv("MYSQL_DATABASE")
	user := os.Getenv("MYSQL_USER")
	pass := os.Getenv("MYSQL_PASS")

	if host == "" || port == "" || dbName == "" || user == "" || pass == "" {
		return "", ErrNoSQLCredentials
	}

	return buildURL(host, port, dbName, user, pass), nil
}

var convertCmd = &cobra.Command{
	Use:   "convert",
	Short: "Convert bolt database to MySQL",
	RunE: func(cmd *cobra.Command, _ []string) error {
		boltPath, err := cmd.Flags().GetString("bolt")
		if err != nil {
			return err
		}

		sqlitePath, err := cmd.Flags().GetString("sqlite")
		if err != nil {
			return err
		}

		sets, boltDB, err := getAllSets(boltPath)
		if err != nil {
			return err
		}

		defer func() {
			err = boltDB.Close()
			if err != nil {
				logger.Error(err.Error())
			}
		}()

		var sqlDB *db.DB

		if sqlitePath != "" {
			sqlDB, err = db.Init("sqlite", sqlitePath)
		} else {
			url, err := BuildSQLURL()
			if err != nil {
				return err
			}
			sqlDB, err = db.Init("mysql", url)

		}
		if err != nil {
			return err
		}

		defer func() {
			err = sqlDB.Close()
			if err != nil {
				logger.Error(err.Error())
			}
		}()

		for _, s := range sets {
			err = transferSet(sqlDB, s)
			if err != nil {
				return err
			}
		}

		return nil
	},
}

func init() {
	convertCmd.Flags().String("bolt", "", "path to the Bolt database file")
	convertCmd.Flags().String("sqlite", "", "path to the SQLite database file")

	if err := convertCmd.MarkFlagRequired("bolt"); err != nil {
		log.Fatal(err)
	}

	RootCmd.AddCommand(convertCmd)
}

func transferSet(sqlDB *db.DB, s *set.Set) error {
	sqlSet, err := convertSet(s)
	if err != nil {
		return err
	}

	err = sqlDB.CreateSet(sqlSet)
	if err != nil {
		return err
	}

	if s.Hide {
		err = sqlDB.SetSetHidden(sqlSet)
		if err != nil {
			return err
		}
	}

	if s.ReadOnly {
		return sqlDB.SetSetReadonly(sqlSet)
	}

	return nil
}

func convertSet(boltSet *set.Set) (*db.Set, error) {

	var sqlSet = &db.Set{}
	var err error

	sqlSet.Name = boltSet.Name
	sqlSet.Requester = boltSet.Requester

	sqlSet.Transformer, err = convertTransformer(boltSet.Transformer)
	if err != nil {
		return nil, err
	}

	sqlSet.MonitorTime = boltSet.MonitorTime
	sqlSet.MonitorRemovals = boltSet.MonitorRemovals
	sqlSet.Description = boltSet.Description
	sqlSet.DeleteLocal = boltSet.DeleteLocal
	//sqlSet.StartedDiscovery = boltSet.StartedDiscovery
	//sqlSet.LastDiscovery = boltSet.LastDiscovery
	//sqlSet.Status = db.Status(boltSet.Status)
	//sqlSet.LastCompleted = boltSet.LastCompleted
	//sqlSet.LastCompletedCount = boltSet.LastCompletedCount
	//sqlSet.LastCompletedSize = boltSet.LastCompletedSize
	//sqlSet.SizeUploaded = boltSet.SizeUploaded
	//sqlSet.SizeRemoved = boltSet.SizeRemoved
	//sqlSet.NumObjectsToBeRemoved = boltSet.NumObjectsToBeRemoved
	//sqlSet.NumObjectsRemoved = boltSet.NumObjectsRemoved

	var reason db.Reason
	err = reason.Set(boltSet.Metadata[transfer.MetaKeyReason])
	if err != nil {
		return nil, err
	}

	sqlSet.Reason = reason

	review, err := time.Parse(time.RFC3339, boltSet.Metadata[transfer.MetaKeyReview])
	if err != nil {
		return nil, fmt.Errorf("%w %s: %s", ErrWrongMetadata, transfer.MetaKeyReview, err)
	}

	sqlSet.ReviewDate = review

	dlt, err := time.Parse(time.RFC3339, boltSet.Metadata[transfer.MetaKeyRemoval])
	if err != nil {
		return nil, fmt.Errorf("%w %s: %s", ErrWrongMetadata, transfer.MetaKeyRemoval, err)
	}

	sqlSet.DeleteDate = dlt

	delete(boltSet.Metadata, transfer.MetaKeyReason)
	delete(boltSet.Metadata, transfer.MetaKeyReview)
	delete(boltSet.Metadata, transfer.MetaKeyRemoval)
	sqlSet.Metadata = boltSet.Metadata

	sqlSet.Error = boltSet.Error
	sqlSet.Warning = boltSet.Warning

	return sqlSet, nil
}

func convertTransformer(transformer string) (*db.Transformer, error) {
	var (
		name    string
		match   string
		replace string
	)

	const regexp = `^/lustre/(scratch[^/]+)(/[^/]*)+?/(projects|teams|users)(_v2)?/([^/]+)/`

	switch transformer {
	case "humgen":
		name = "humgen"
		match = regexp
		replace = "/humgen/$3/$5/$1$4/"
	case "gengen":
		name = "gengen"
		match = regexp
		replace = "/humgen/gengen/$3/$5/$1$4/"
	default:
		name = transformer

		rule := strings.TrimPrefix(transformer, "prefix=")
		parts := strings.Split(rule, ":")
		if len(parts) != 2 {
			err := fmt.Errorf("%w: %s", ErrWrongTransformer, transformer)
			return nil, err
		}

		match = "^" + parts[0]
		replace = parts[1]
	}

	return db.NewTransformer(name, match, replace)
}
