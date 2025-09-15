/*******************************************************************************
 * Copyright (c) 2025 Genome Research Ltd.
 *
 * Author: Iaroslav Popov <ip13@sanger.ac.uk>
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *
 * The above copyright notice and this permission notice shall be included
 * in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY
 * CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT,
 * TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
 * SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 ******************************************************************************/

package main

import (
	"bytes"
	"database/sql"
	"errors"
	"fmt"
	"github.com/wtsi-hgi/ibackup/transfer"
	"maps"
	"math/rand"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	_ "modernc.org/sqlite"

	. "github.com/smartystreets/goconvey/convey"
	"github.com/wtsi-hgi/ibackup-devkit/cmd"
	"github.com/wtsi-hgi/ibackup/db"
	"github.com/wtsi-hgi/ibackup/set"
)

const (
	_     = iota // ignore first value (0)
	_ int = 1 << (10 * iota)
	_
	GB
)

func TestBolt(t *testing.T) {
	Convey("You cannot run a tool without --database flag", t, func() {
		cmd.RootCmd.SetArgs([]string{"bolt"})

		cmdErrs := new(bytes.Buffer)
		cmd.RootCmd.SetErr(cmdErrs)
		cmd.RootCmd.SetOut(cmdErrs)

		err := cmd.RootCmd.Execute()
		So(err, ShouldNotBeNil)
		So(err.Error(), ShouldContainSubstring, "required flag(s) \"database\" not set")
	})

	Convey("Given a test database file", t, func() {
		testDBFile := filepath.Join(t.TempDir(), "test.db")
		bolt, err := set.New(testDBFile, "", false)
		So(err, ShouldBeNil)
		for i := range 5 {
			setName := "set-" + strconv.Itoa(i)
			s := &set.Set{
				Name:     setName,
				ReadOnly: i%2 == 0,
			}
			err = bolt.AddOrUpdate(s)
			So(err, ShouldBeNil)
		}

		err = bolt.Close()
		So(err, ShouldBeNil)

		cmdErrs := new(bytes.Buffer)
		cmd.RootCmd.SetOut(cmdErrs)

		Convey("You cannot run a tool on a non-existing database", func() {
			cmd.RootCmd.SetArgs([]string{"bolt", "--database", filepath.Join(t.TempDir(), "invalid"), "--lock-all-sets"})
			err := cmd.RootCmd.Execute()
			So(err.Error(), ShouldContainSubstring, "no such file or directory")
		})

		getAllSets := func() ([]*set.Set, *set.DB) {
			bolt, err := set.New(testDBFile, "", false)
			So(err, ShouldBeNil)

			allSets, err := bolt.GetAll()
			So(err, ShouldBeNil)

			return allSets, bolt
		}

		Convey("You can hide read-only sets", func() {
			cmd.RootCmd.SetArgs([]string{"bolt", "--database", testDBFile, "--hide-readonly"})

			err := cmd.RootCmd.Execute()
			So(err, ShouldBeNil)

			allSets, bolt := getAllSets()

			for _, s := range allSets {
				if s.ReadOnly {
					So(s.Hide, ShouldBeTrue)
				} else {
					So(s.Hide, ShouldBeFalse)
				}
			}

			err = bolt.Close()
			So(err, ShouldBeNil)
		})

		Convey("You can make all sets read-only", func() {
			cmd.RootCmd.SetArgs([]string{"bolt", "--database", testDBFile, "--lock-all-sets"})

			err := cmd.RootCmd.Execute()
			So(err, ShouldBeNil)

			Convey("And all sets will be read-only", func() {
				allSets, bolt := getAllSets()

				for _, s := range allSets {
					So(s.ReadOnly, ShouldBeTrue)
				}

				err = bolt.Close()
				So(err, ShouldBeNil)

				Convey("Then you can hide the newly read-only sets as well", func() {
					cmd.RootCmd.SetArgs([]string{"bolt", "--database", testDBFile, "--hide-readonly"})

					err := cmd.RootCmd.Execute()
					So(err, ShouldBeNil)

					allSets, bolt = getAllSets()

					for _, s := range allSets {
						So(s.Hide, ShouldBeTrue)
					}

					err = bolt.Close()
					So(err, ShouldBeNil)
				})
			})
		})
	})
}

const (
	testSetsNum    = 5
	maxFilesPerSet = 5
)

func TestConvert(t *testing.T) {
	cmdErrs := new(bytes.Buffer)
	cmd.RootCmd.SetOut(cmdErrs)

	Convey("Given a test SQL database", t, func() {
		resetDatabase(t)

		url, err := cmd.BuildSQLURL()
		if errors.Is(err, cmd.ErrNoSQLCredentials) {
			t.Skip("skipping test: no SQL credentials")
		}
		So(err, ShouldBeNil)

		sqlDB, err := db.Init("mysql", url)
		So(err, ShouldBeNil)

		t.Cleanup(func() {
			callAndLogError(t, sqlDB.Close)
		})

		Convey("And a connection to a Bolt database", func() {
			testBoltFile := filepath.Join(t.TempDir(), "test.db")
			boltDB, err := set.New(testBoltFile, "", false)
			So(err, ShouldBeNil)

			Convey("With a not complete set", func() {
				testSet := generateRandomSets(1)[0]
				testSet.Status = randomChoice(set.PendingDiscovery, set.PendingUpload, set.Uploading, set.Failing)

				err = boltDB.AddOrUpdate(testSet)
				So(err, ShouldBeNil)

				err = boltDB.Close()
				So(err, ShouldBeNil)

				Convey("Transfer will fail", func() {
					cmd.RootCmd.SetArgs([]string{"convert", "--bolt", testBoltFile})

					err = cmd.RootCmd.Execute()
					So(err, ShouldNotBeNil)
					So(err.Error(), ShouldContainSubstring, cmd.ErrSetNotComplete.Error())
				})
			})

			Convey("With a set with a not complete file", func() {
				testSet := generateRandomSets(1)[0]

				err = boltDB.AddOrUpdate(testSet)
				So(err, ShouldBeNil)

				setFiles := generateRandomFiles(1, "/")

				err = boltDB.MergeFileEntries(testSet.ID(), setFiles)
				So(err, ShouldBeNil)

				entry, err := boltDB.GetFileEntryForSet(testSet.ID(), setFiles[0])
				So(err, ShouldBeNil)

				entry.Status = randomChoice(set.Registered, set.Pending, set.UploadingEntry)

				err = boltDB.UpdateEntry(testSet.ID(), setFiles[0], entry)
				So(err, ShouldBeNil)

				err = boltDB.Close()
				So(err, ShouldBeNil)

				Convey("Transfer will fail", func() {
					cmd.RootCmd.SetArgs([]string{"convert", "--bolt", testBoltFile})

					err = cmd.RootCmd.Execute()
					So(err, ShouldNotBeNil)
					So(err.Error(), ShouldContainSubstring, cmd.ErrSetNotComplete.Error())
				})
			})

			Convey("With only complete sets", func() {
				testSets := generateRandomSets(testSetsNum)

				filesMap := make(map[*set.Set][]*set.Entry)

				for i, s := range testSets {
					isReadOnly := s.ReadOnly
					s.ReadOnly = false

					err = boltDB.AddOrUpdate(s)
					So(err, ShouldBeNil)

					var prefix string

					scratchNumbers := []int{119, 120, 122, 123, 124, 125, 126, 127}
					scratch := fmt.Sprintf("scratch%d", randomChoice(scratchNumbers...))

					switch s.Transformer {
					case "humgen":
						prefix = "/lustre/" + scratch + "/humgen/projects_v2/"
					case "gengen":
						prefix = "/lustre/" + scratch + "/gengen/projects_v2/"
					default:
						prefix = "/lustre/" + scratch
					}

					setFiles := generateRandomFiles(rand.Intn(maxFilesPerSet), prefix)

					if len(setFiles) > 0 {
						err = boltDB.MergeFileEntries(s.ID(), setFiles)
						So(err, ShouldBeNil)
					}

					entries := setRandomFileProperties(t, boltDB, s, setFiles)
					testSets[i] = updateSetProperties(t, boltDB, s, isReadOnly, entries)

					filesMap[s] = entries
				}

				// at this point you may have the same path in two different sets with two different inodes
				// this is valid and reflects the situation when a backed-up file was deleted and recreated

				printTestSetup(filesMap)

				err = boltDB.Close()
				So(err, ShouldBeNil)

				Convey("You can transfer sets", func() {
					cmd.RootCmd.SetArgs([]string{"convert", "--bolt", testBoltFile})

					err = cmd.RootCmd.Execute()
					So(err, ShouldBeNil)

					for _, s := range testSets {
						t.Logf("\ncheck set %s", s.Name)

						// we do not transfer empty sets
						if len(filesMap[s]) == 0 {
							_, err = sqlDB.GetSet(s.Name, s.Requester)
							So(err, ShouldNotBeNil)
							So(err.Error(), ShouldContainSubstring, "no rows in result set")

							continue
						}

						newSet, err := sqlDB.GetSet(s.Name, s.Requester)
						So(err, ShouldBeNil)

						checkSetsIdentical(t, s, newSet)

						newFiles := collectIter(t, sqlDB.GetSetFiles(newSet))
						checkFilesIdentical(t, filesMap[s], newFiles)
					}
				})
			})
		})
	})
}

func generateRandomSets(n int) []*set.Set {
	testSets := make([]*set.Set, n)

	for i := range n {
		var monitorTime time.Duration

		isMonitored := randomChoice(true, false)
		if isMonitored {
			monitorTime = randomDuration()
		}

		s := &set.Set{
			Name:        fmt.Sprintf("set-%d", i),
			Requester:   "test-user",
			Transformer: randomChoice("humgen", "gengen", "prefix=/lustre:/humgen"),
			Metadata: map[string]string{
				transfer.MetaKeyReason:  randomChoice("backup", "archive", "quarantine"),
				transfer.MetaKeyReview:  randomDate(),
				transfer.MetaKeyRemoval: randomDate(),
			},
			ReadOnly:        randomChoice(true, false),
			Hide:            randomChoice(true, false),
			Status:          set.Complete,
			MonitorTime:     monitorTime,
			MonitorRemovals: randomChoice(true, false),
			Description:     randomString(),
		}

		testSets[i] = s
	}

	return testSets
}

func randomChoice[T any](options ...T) T {
	return options[rand.Intn(len(options))]
}

func randomDate() string {
	return time.Now().AddDate(0, 0, rand.Intn(360)).Format(time.RFC3339)
}

func randomDuration() time.Duration {
	return time.Duration(rand.Intn(30)) * 24 * time.Hour
}

func randomString() string {
	const (
		maxLength = 10
		charset   = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	)

	length := rand.Intn(maxLength)

	result := make([]byte, length)
	for i := range length {
		result[i] = charset[rand.Intn(len(charset))]
	}

	return string(result)
}

func resetDatabase(t *testing.T) {
	t.Helper()

	url, err := cmd.BuildSQLURL()
	if errors.Is(err, cmd.ErrNoSQLCredentials) {
		t.Skip("skipping test: no SQL credentials")
	}
	So(err, ShouldBeNil)

	sqlDB, err := sql.Open("mysql", url)
	So(err, ShouldBeNil)

	t.Cleanup(func() {
		callAndLogError(t, sqlDB.Close)
	})

	for _, table := range [...]string{"changedInodes", "activeDiscoveries", "queue",
		"processes", "localFiles", "remoteFiles", "hardlinks", "toDiscover",
		"sets", "transformers"} {
		_, err = sqlDB.Exec("DROP TABLE IF EXISTS `" + table + "`;")
		So(err, ShouldBeNil)
	}
}

func checkSetsIdentical(t *testing.T, s1 *set.Set, s2 *db.Set) {
	t.Helper()

	meta := maps.Clone(s1.Metadata)
	delete(meta, transfer.MetaKeyReason)
	delete(meta, transfer.MetaKeyReview)
	delete(meta, transfer.MetaKeyRemoval)

	// check set properties
	So(s2.Name, ShouldEqual, s1.Name)
	So(s2.Requester, ShouldEqual, s1.Requester)
	So(s2.MonitorTime, ShouldEqual, s1.MonitorTime/time.Second)
	So(s2.MonitorRemovals, ShouldEqual, s1.MonitorRemovals)
	So(s2.Description, ShouldEqual, s1.Description)
	So(map[string]string(s2.Metadata), ShouldResemble, meta)
	So(s2.DeleteLocal, ShouldEqual, s1.DeleteLocal)
	So(s2.Hidden, ShouldEqual, s1.Hide)
	So(s2.Status, ShouldEqual, db.Status(s1.Status))

	// check file counts
	So(s2.NumFiles, ShouldEqual, s1.NumFiles)
	So(s2.Uploaded, ShouldEqual, s1.Uploaded)
	So(s2.Skipped, ShouldEqual, s1.Skipped)
	So(s2.Replaced, ShouldEqual, s1.Replaced)
	So(s2.Missing, ShouldEqual, s1.Missing)
	So(s2.Orphaned, ShouldEqual, s1.Orphaned)
	So(s2.Abnormal, ShouldEqual, s1.Abnormal)
	So(s2.Failed, ShouldEqual, s1.Failed)

	// check discovery attributes
	//So(s2.StartedDiscovery, ShouldEqual, s1.StartedDiscovery)
	//So(s2.LastDiscovery, ShouldEqual, s1.LastDiscovery)

	// check completion attributes
	//So(s2.LastCompleted, ShouldEqual, s1.LastCompleted)
	//So(s2.LastCompletedCount, ShouldEqual, s1.LastCompletedCount)
	//So(s2.LastCompletedSize, ShouldEqual, s1.LastCompletedSize)
	//So(s2.SizeUploaded, ShouldEqual, s1.SizeUploaded)

	// check removal properties
	So(s2.SizeRemoved, ShouldEqual, s1.SizeRemoved)
	So(s2.NumObjectsToBeRemoved, ShouldEqual, s1.NumObjectsToBeRemoved)
	So(s2.NumObjectsRemoved, ShouldEqual, s1.NumObjectsRemoved)

	// check errors
	So(s2.Error, ShouldEqual, s1.Error)
	So(s2.Warning, ShouldEqual, s1.Warning)
}

func generateRandomFiles(n int, prefix string) []string {
	files := make([]string, n)

	for i := range n {
		dir1 := fmt.Sprintf("project%d", rand.Intn(i+1))
		filename := fmt.Sprintf("file%d.txt", i)
		files[i] = filepath.Join(prefix, dir1, filename)
	}

	return files
}

func checkFilesIdentical(t *testing.T, files1 []*set.Entry, files2 []*db.File) {
	t.Helper()

	So(files2, ShouldHaveLength, len(files1))

	fileMap1 := make(map[string]*set.Entry, len(files1))
	for _, file := range files1 {
		fileMap1[file.Path] = file
	}

	fileMap2 := make(map[string]*db.File, len(files2))
	for _, file := range files2 {
		fileMap2[file.LocalPath] = file
	}

	for _, file := range files2 {
		fileMatch, ok := fileMap1[file.LocalPath]
		So(ok, ShouldBeTrue)

		So(file.LocalPath, ShouldEqual, fileMatch.Path)
		So(file.Size, ShouldEqual, fileMatch.Size)
		So(file.Inode, ShouldEqual, fileMatch.Inode)
		So(FileTypeToString(file.Type), ShouldEqual, EntryTypeToString(fileMatch.Type))
		if fileMatch.Status == set.Failed || fileMatch.Status == set.AbnormalEntry {
			So(FileStatusToString(file.Status), ShouldBeBlank)
		} else {
			So(FileStatusToString(file.Status), ShouldEqual, fileMatch.Status.String())
			SkipSo(file.LastUpload, ShouldEqual, fileMatch.LastAttempt) // TODO
		}
	}
}

func FileStatusToString(status db.FileStatus) string {
	switch status {
	case db.StatusMissing:
		return "missing"
	case db.StatusOrphaned:
		return "orphaned"
	case db.StatusUploaded:
		return "uploaded"
	case db.StatusReplaced:
		return "replaced"
	case db.StatusSkipped:
		return "skipped"
	default:
		return ""
	}
}

func EntryTypeToString(status set.EntryType) string {
	switch status {
	case set.Regular:
		return "regular"
	case set.Hardlink:
		return "hardlink"
	case set.Symlink:
		return "symlink"
	case set.Abnormal:
		return "abnormal"
	case set.Directory:
		return "directory"
	case set.Unknown:
		return "unknown"
	default:
		return ""
	}
}

func FileTypeToString(status db.FileType) string {
	switch status {
	case db.Regular:
		return "regular"
	case db.Hardlink:
		return "hardlink"
	case db.Symlink:
		return "symlink"
	case db.Abnormal:
		return "abnormal"
	case db.Directory:
		return "directory"
	case db.Unknown:
		return "unknown"
	default:
		return ""
	}
}

func collectIter[T any](t *testing.T, i *db.IterErr[T]) []T {
	t.Helper()

	var vs []T

	err := i.ForEach(func(item T) error {
		vs = append(vs, item)

		return nil
	})
	So(err, ShouldBeNil)

	return vs
}

func callAndLogError(t *testing.T, f func() error) {
	t.Helper()

	err := f()
	if err != nil {
		t.Log(err)
	}
}

func setRandomFileProperties(t *testing.T, boltDB *set.DB, s *set.Set, files []string) []*set.Entry {
	t.Helper()

	entryTypes := []set.EntryType{
		set.Regular, set.Symlink, set.Abnormal, set.Unknown,
	}

	entryTypesWithHardlink := append(entryTypes, set.Hardlink)

	entryStatuses := []set.EntryStatus{
		set.Uploaded, set.Failed, set.Replaced, set.Skipped, set.Orphaned,
	}

	entries := make([]*set.Entry, len(files))

	for i, file := range files {

		entry, err := boltDB.GetFileEntryForSet(s.ID(), file)
		So(err, ShouldBeNil)

		randomStatus := randomChoice(entryStatuses...)

		entry.Status = set.Pending

		err = boltDB.UpdateEntry(s.ID(), file, entry)
		So(err, ShouldBeNil)

		request := &transfer.Request{
			Local:     entry.Path,
			Requester: s.Requester,
			Set:       s.Name,
		}

		if randomStatus == set.Failed {
			request.Status = transfer.RequestStatusFailed
		}

		// need it to set entry.newSize = true and entry.newFail = true
		entry, err = boltDB.SetEntryStatus(request)
		So(err, ShouldBeNil)

		if i == 0 || countRegularEntries(entries) == 0 {
			entry.Type = randomChoice(entryTypes...)
		} else {
			entry.Type = randomChoice(entryTypesWithHardlink...)
		}

		switch entry.Type {
		case set.Abnormal:
			entry.Status = set.AbnormalEntry
		case set.Unknown:
			entry.Status = set.Missing
		default:
			entry.Status = randomStatus
		}

		entry.Size = uint64(rand.Intn(GB))
		entry.Inode = uint64(rand.Int31())

		if entry.Type == set.Hardlink {
			randomEntry := pickRandomEntry(entries, set.Regular)
			entry.Inode = randomEntry.Inode
			entry.Size = randomEntry.Size
		}

		err = boltDB.UpdateEntry(s.ID(), file, entry)
		So(err, ShouldBeNil)

		entries[i] = entry
	}

	return entries
}

func countRegularEntries(entries []*set.Entry) int {
	var count int

	for _, entry := range entries {
		if entry == nil {
			continue
		}

		if entry.Type == set.Regular {
			count++
		}
	}

	return count
}

func pickRandomEntry(entries []*set.Entry, entryType set.EntryType) *set.Entry {
	choices := make([]int, 0, len(entries))

	for i, entry := range entries {
		if entry == nil {
			continue
		}

		if entry.Type == entryType {
			choices = append(choices, i)
		}
	}

	index := randomChoice(choices...)

	return entries[index]
}

func updateSetProperties(t *testing.T, boltDB *set.DB, s *set.Set, isReadOnly bool, entries []*set.Entry) *set.Set {
	t.Helper()

	for _, entry := range entries {
		s.NumFiles++

		err := s.UpdateBasedOnEntry(entry, boltDB.GetFileEntries)
		So(err, ShouldBeNil)
	}

	s.ReadOnly = isReadOnly

	err := boltDB.AddOrUpdate(s)
	So(err, ShouldBeNil)

	return s
}

func printTestSetup(filesMap map[*set.Set][]*set.Entry) {
	fmt.Println("\n\ntest setup:")
	for s, files := range filesMap {
		fmt.Printf("set %s\n", s.Name)
		for _, file := range files {
			fmt.Printf("  %s - %s - %s\n", file.Path, EntryTypeToString(file.Type), file.Status)
		}
	}
}
