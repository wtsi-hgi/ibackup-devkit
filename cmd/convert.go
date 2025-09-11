package cmd

import (
	"errors"
	"fmt"
	"github.com/spf13/cobra"
	"github.com/wtsi-hgi/ibackup/transfer"
	"log"
	"os"
	"slices"
	"strings"
	"time"

	"github.com/wtsi-hgi/ibackup/db"
	"github.com/wtsi-hgi/ibackup/set"
)

const maxAttempts = 3

var (
	ErrWrongTransformer = errors.New("wrong transformer")
	ErrNoSQLCredentials = errors.New("connection details for MySQL are not set")
	ErrWrongMetadata    = errors.New("wrong metadata value for key")
	ErrWrongType        = errors.New("unknown type")
	ErrWrongStatus      = errors.New("unknown status")
	ErrSetNotComplete   = errors.New("set is not complete")
	ErrEmptySet         = errors.New("set is empty")
	ErrWrongReason      = errors.New("unknown reason")
)

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

		defer callAndLogError(boltDB.Close)

		err = validateSets(sets)
		if err != nil {
			return err
		}

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

		defer callAndLogError(sqlDB.Close)
		defer callAndLogError(sqlDB.RemoveStaleProcesses)

		for _, s := range sets {
			logger.Info("Transferring set: %s of %s", s.Name, s.Requester)
			err = validateSet(s, boltDB)
			if err != nil {
				if errors.Is(err, ErrEmptySet) {
					logger.Warn("%w - set will not be transferred", err)

					continue
				}

				return err
			}

			err = transferAllDataForSet(boltDB, sqlDB, s)
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

// validateSets validates external set properties
func validateSets(sets []*set.Set) error {
	for _, s := range sets {
		if s.Status != set.Complete {
			return fmt.Errorf("%w: %s/%s - %s", ErrSetNotComplete, s.Requester, s.Name, s.Status)
		}
	}

	return nil
}

// validateSet validates internal set properties
func validateSet(s *set.Set, boltDB *set.DB) error {
	files, err := boltDB.GetPureFileEntries(s.ID())
	if err != nil {
		return err
	}

	if s.NumFiles == 0 && len(files) == 0 {
		return fmt.Errorf("%w: %s/%s", ErrEmptySet, s.Requester, s.Name)
	}

	for _, file := range files {
		switch file.Status {
		case set.Registered, set.Pending, set.UploadingEntry:
			return fmt.Errorf("%w: %s/%s file %s - %s", ErrSetNotComplete, s.Requester, s.Name, file.Path, file.Status)
		default:
		}
	}

	return nil
}

func transferAllDataForSet(boltDB *set.DB, sqlDB *db.DB, s *set.Set) error {
	newSet, err := transferSet(sqlDB, s)
	if err != nil {
		return err
	}

	err = transferFiles(boltDB, sqlDB, s)
	if err != nil {
		return err
	}

	if s.ReadOnly {
		err = sqlDB.SetSetReadonly(newSet)
	}

	return err
}

func transferSet(sqlDB *db.DB, s *set.Set) (*db.Set, error) {
	sqlSet, err := convertSet(s)
	if err != nil {
		return nil, err
	}

	err = sqlDB.CreateSet(sqlSet)
	if err != nil {
		return nil, err
	}

	if s.Hide {
		err = sqlDB.SetSetHidden(sqlSet)
	}

	return sqlSet, err
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

	sqlSet.MonitorTime = boltSet.MonitorTime / time.Second
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

	sqlSet.Reason, err = convertReason(boltSet.Metadata[transfer.MetaKeyReason])
	if err != nil {
		return nil, err
	}

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

func convertReason(reason string) (db.Reason, error) {
	var value db.Reason

	switch reason {
	case db.Backup.String():
		value = db.Backup
	case db.Archive.String():
		value = db.Archive
	case db.Quarantine.String():
		value = db.Quarantine
	default:
		return value, fmt.Errorf("%w: %s", ErrWrongReason, reason)
	}

	return value, nil
}

// I need a 2-staged transfer. First I partially transfer not-complete files (replaced, orphaned),
// then I transfer all files
func transferFiles(boltDB *set.DB, sqlDB *db.DB, s *set.Set) error {
	files, err := boltDB.GetPureFileEntries(s.ID())
	if err != nil {
		return err
	}

	newSet, err := sqlDB.GetSet(s.Name, s.Requester)
	if err != nil {
		return err
	}

	normalFiles := make([]*db.File, 0, len(files))
	orphanedFiles := make([]*db.File, 0, len(files))
	replacedFiles := make([]*db.File, 0, len(files))
	failedFiles := make([]*db.File, 0, len(files))

	correctMTimes := make(map[string]int64)

	for _, file := range files {
		newFile, err := convertFile(file, boltDB)
		if err != nil {
			return err
		}

		switch file.Status {
		case set.Orphaned:
			newFile.Status = db.StatusUploaded
			orphanedFiles = append(orphanedFiles, newFile)
		case set.Replaced:
			correctMTimes[newFile.LocalPath] = newFile.Mtime
			newFile.Mtime = -1
			replacedFiles = append(replacedFiles, newFile)
		case set.Failed:
			failedFiles = append(failedFiles, newFile)
		default:
			normalFiles = append(normalFiles, newFile)
		}

		d := db.Discover{
			Path: newFile.LocalPath,
			Type: db.DiscoverFile,
		}

		err = sqlDB.AddSetDiscovery(newSet, &d)
		if err != nil {
			return err
		}
	}

	p, err := sqlDB.RegisterProcess()
	if err != nil {
		return err
	}

	notCompleteFiles := slices.Concat(replacedFiles, orphanedFiles)
	err = uploadFiles(p, newSet, notCompleteFiles, files, sqlDB)
	if err != nil {
		return err
	}

	for _, file := range orphanedFiles {
		file.Status = db.StatusMissing
	}

	for _, file := range replacedFiles {
		file.Mtime = correctMTimes[file.LocalPath]
	}

	allFiles := slices.Concat(normalFiles, notCompleteFiles, failedFiles)
	err = uploadFiles(p, newSet, allFiles, files, sqlDB)
	if err != nil {
		return err
	}

	return nil
}

func uploadFiles(p *db.Process, s *db.Set, newFiles []*db.File, oldFiles []*set.Entry, sqlDB *db.DB) error {
	if len(newFiles) == 0 {
		return nil
	}

	err := sqlDB.CompleteDiscovery(s, slices.Values(newFiles), noSeq[*db.File])
	if err != nil {
		return err
	}

	for range maxAttempts {
		err = transferFileStatuses(p, sqlDB, oldFiles)
		if err != nil {
			return err
		}
	}

	return nil
}

func transferFileStatuses(p *db.Process, sqlDB *db.DB, files []*set.Entry) error {
	itErr := sqlDB.ReserveTasks(p, len(files))

	var tasks []*db.Task
	err := itErr.ForEach(func(task *db.Task) error {
		tasks = append(tasks, task)
		return nil
	})
	if err != nil {
		return err
	}

	for _, task := range tasks {
		file := matchFile(task, files)
		switch file.Status {
		case set.Failed:
			err = sqlDB.TaskFailed(task)
		case set.Skipped:
			task.Skipped = true
			err = sqlDB.TaskComplete(task)
		case set.Uploaded, set.Replaced, set.Orphaned, set.AbnormalEntry:
			err = sqlDB.TaskComplete(task)
		default:
			err = fmt.Errorf("%w: Cannot handle file %s with status %s", ErrWrongStatus, file.Path, file.Status)
		}
	}

	return err
}

func transferFailedFiles(p *db.Process, s *db.Set, newFiles []*db.File, oldFiles []*set.Entry, sqlDB *db.DB) error {
	err := sqlDB.CompleteDiscovery(s, slices.Values(newFiles), noSeq[*db.File])
	if err != nil {
		return err
	}

	for range maxAttempts {
		err = transferFileStatuses(p, sqlDB, oldFiles)
		if err != nil {
			return err
		}
	}

	return nil
}

func matchFile(task *db.Task, files []*set.Entry) *set.Entry {
	for _, file := range files {
		if file.Path == task.LocalPath {
			return file
		}
	}

	return nil
}

func noSeq[T any](_ func(T) bool) {}

func convertFile(file *set.Entry, boltDB *set.DB) (*db.File, error) {
	newType, err := convertFileType(file.Type)
	if err != nil {
		return nil, err
	}

	newStatus, err := convertFileStatus(file.Status)
	if err != nil {
		return nil, err
	}

	mountPoint := boltDB.GetMountPointFromPath(file.Path)
	if mountPoint == "/" {
		if strings.HasPrefix(file.Path, "/lustre/scratch123") {
			mountPoint = "/lustre/scratch123"
		} else if strings.HasPrefix(file.Path, "/lustre/scratch119") {
			mountPoint = "/lustre/scratch119"
		} else {
			return nil, fmt.Errorf("cannot convert file %s: mount point is root", file.Path)
		}
	}

	newFile := &db.File{
		//RemotePath: "", // set inside SQL call
		LocalPath:  file.Path,
		Size:       file.Size,
		Inode:      file.Inode,
		MountPount: mountPoint,
		Type:       newType,
		Status:     newStatus,
		// Btime: 0, no way to obtain
		// Mtime: 0, no way to obtain
		// Owner: "", no way to obtain
		// Group: "", no way to obtain
	}

	if file.Type == set.Symlink {
		newFile.SymlinkDest = ""
	}

	return newFile, nil
}

func convertFileType(t set.EntryType) (db.FileType, error) {
	var newType db.FileType

	switch t {
	case set.Regular:
		newType = db.Regular
	case set.Hardlink:
		newType = db.Hardlink
	case set.Symlink:
		newType = db.Symlink
	case set.Directory:
		newType = db.Directory
	case set.Abnormal:
		newType = db.Abnormal
	case set.Unknown:
		newType = db.Unknown
	default:
		return newType, fmt.Errorf("%w: %d", ErrWrongType, t)
	}

	return newType, nil
}

func convertFileStatus(status set.EntryStatus) (db.FileStatus, error) {
	var newStatus db.FileStatus

	switch status {
	case set.Uploaded:
		newStatus = db.StatusUploaded
	case set.Failed:
		newStatus = db.StatusNone
	case set.Missing:
		newStatus = db.StatusMissing
	case set.AbnormalEntry:
		newStatus = db.StatusNone
	case set.Replaced:
		newStatus = db.StatusReplaced
	case set.Skipped:
		newStatus = db.StatusSkipped
	case set.Orphaned:
		newStatus = db.StatusOrphaned
	default:
		return newStatus, fmt.Errorf("%w: %d", ErrWrongStatus, status)
	}

	return newStatus, nil
}

//func handleSetStatus(sqlDB *db.DB, sqlSet *db.Set) error {
//	switch sqlSet.Status {
//	case db.PendingDiscovery:
//		return nil
//	case db.PendingUpload:
//		return nil
//	case db.Uploading:
//		return nil
//	case db.Failing:
//		return nil
//	case db.Complete:
//		return sqlDB.CompleteDiscovery(sqlSet)
//	default:
//		return fmt.Errorf("%w: %d", ErrWrongStatus, sqlSet.Status)
//	}
//}

func callAndLogError(f func() error) {
	err := f()
	if err != nil {
		logger.Error(err.Error())
	}
}
