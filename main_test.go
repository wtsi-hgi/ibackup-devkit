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
	_      = iota // ignore first value (0)
	KB int = 1 << (10 * iota)
	MB
	GB
	TB
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

//func TestConvert(t *testing.T) {
//	cmdErrs := new(bytes.Buffer)
//	cmd.RootCmd.SetOut(cmdErrs)
//
//	Convey("Given an empty MySQL database", t, func() {
//		url, err := cmd.BuildSQLURL()
//		So(err, ShouldBeNil)
//
//		resetDatabase(t)
//
//		sqlDB, err := db.Init("mysql", url)
//		So(err, ShouldBeNil)
//
//		defer callAndLogError(t, sqlDB.Close)
//
//		Convey("And an empty Bolt database", func() {
//			testBoltFile := filepath.Join(t.TempDir(), "test.db")
//			boltDB, err := set.New(testBoltFile, "", false)
//			So(err, ShouldBeNil)
//
//			Convey("You can transfer empty sets", func() {
//				testSets := generateRandomSets(testSetsNum)
//				for _, s := range testSets {
//					s.Status = set.Complete
//
//					err = boltDB.AddOrUpdate(s)
//					So(err, ShouldBeNil)
//				}
//
//				err = boltDB.Close()
//				So(err, ShouldBeNil)
//
//				cmd.RootCmd.SetArgs([]string{"convert", "--bolt", testBoltFile})
//
//				err = cmd.RootCmd.Execute()
//				So(err, ShouldBeNil)
//
//				for _, s := range testSets {
//					Convey(fmt.Sprintf("check set %s", s.Name), func() {
//						newSet, err := sqlDB.GetSet(s.Name, s.Requester)
//						So(err, ShouldBeNil)
//
//						checkSetsIdentical(t, s, newSet)
//					})
//				}
//			})
//		})
//	})
//}

func TestConvert(t *testing.T) {
	cmdErrs := new(bytes.Buffer)
	cmd.RootCmd.SetOut(cmdErrs)

	Convey("Given a test SQL database", t, func() {
		url, err := cmd.BuildSQLURL()
		So(err, ShouldBeNil)

		sqlDB, err := db.Init("mysql", url)
		So(err, ShouldBeNil)

		defer callAndLogError(t, sqlDB.Close)

		resetDatabase(t)

		Convey("And a connection to a Bolt database", func() {
			testBoltFile := filepath.Join(t.TempDir(), "test.db")
			boltDB, err := set.New(testBoltFile, "", false)
			So(err, ShouldBeNil)

			Convey("With a not complete set", func() {
				testSet := generateRandomSets(1)[0]
				testSet.Status = randomChoice(set.PendingDiscovery, set.PendingUpload, set.Uploading)

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

				filesMap := make(map[*set.Set][]string)

				for _, s := range testSets {
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
					filesMap[s] = setFiles

					if len(setFiles) > 0 {
						err = boltDB.MergeFileEntries(s.ID(), setFiles)
						So(err, ShouldBeNil)
					}

					setRandomFileProperties(t, boltDB, s, setFiles)
				}

				err = boltDB.Close()
				So(err, ShouldBeNil)

				Convey("You can transfer sets", func() {
					cmd.RootCmd.SetArgs([]string{"convert", "--bolt", testBoltFile})

					err = cmd.RootCmd.Execute()
					So(err, ShouldBeNil)

					for _, s := range testSets {
						Convey(fmt.Sprintf("check set %s", s.Name), func() {
							newSet, err := sqlDB.GetSet(s.Name, s.Requester)
							So(err, ShouldBeNil)

							checkSetsIdentical(t, s, newSet)

							newFiles := collectIter(t, sqlDB.GetSetFiles(newSet))
							checkFilesIdentical(t, filesMap[s], newFiles)
						})
					}
				})
			})
		})
	})
}

func generateRandomSets(n int) []*set.Set {
	testSets := make([]*set.Set, n)

	for i := range n {
		setName := fmt.Sprintf("set-%d", i)
		s := &set.Set{
			Name:        setName,
			Requester:   "test-user",
			Transformer: randomChoice("humgen", "gengen", "prefix=/lustre:/humgen"),
			Metadata: map[string]string{
				transfer.MetaKeyReason:  randomChoice("backup", "archive", "quarantine"),
				transfer.MetaKeyReview:  randomDate(),
				transfer.MetaKeyRemoval: randomDate(),
			},
			ReadOnly: randomChoice(true, false),
			Hide:     randomChoice(true, false),
			Status:   randomChoice(set.Failing, set.Complete),
		}

		testSets[i] = s
	}

	return testSets
}

func randomChoice[T any](options ...T) T {
	return options[rand.Intn(len(options))]
}

func randomSubset[T comparable](options []T, n int) []T {
	subset := make([]T, n)

	for i := range n {
		subset[i] = randomChoice(options...)
		options = removeValue(options, subset[i])
	}
	return subset
}

func randomDate() string {
	return time.Now().AddDate(0, 0, rand.Intn(360)).Format(time.RFC3339)
}

func resetDatabase(t *testing.T) {
	t.Helper()

	url, err := cmd.BuildSQLURL()
	So(err, ShouldBeNil)

	sqlDB, err := sql.Open("mysql", url)
	So(err, ShouldBeNil)

	defer callAndLogError(t, sqlDB.Close)

	for _, table := range [...]string{"activeDiscoveries", "queue",
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

	So(s2.Name, ShouldEqual, s1.Name)
	So(s2.Requester, ShouldEqual, s1.Requester)
	So(s2.MonitorTime, ShouldEqual, s1.MonitorTime)
	So(s2.MonitorRemovals, ShouldEqual, s1.MonitorRemovals)
	So(s2.Description, ShouldEqual, s1.Description)
	So(map[string]string(s2.Metadata), ShouldResemble, meta)
	So(s2.DeleteLocal, ShouldEqual, s1.DeleteLocal)
	So(s2.StartedDiscovery, ShouldEqual, s1.StartedDiscovery)
	//So(s2.LastDiscovery, ShouldEqual, s1.LastDiscovery)
	So(s2.Status, ShouldEqual, db.Status(s1.Status))
	//So(s2.LastCompleted, ShouldEqual, s1.LastCompleted)
	So(s2.LastCompletedCount, ShouldEqual, s1.LastCompletedCount)
	So(s2.LastCompletedSize, ShouldEqual, s1.LastCompletedSize)
	So(s2.SizeUploaded, ShouldEqual, s1.SizeUploaded)
	So(s2.SizeRemoved, ShouldEqual, s1.SizeRemoved)
	So(s2.NumObjectsToBeRemoved, ShouldEqual, s1.NumObjectsToBeRemoved)
	So(s2.NumObjectsRemoved, ShouldEqual, s1.NumObjectsRemoved)
	So(s2.Error, ShouldEqual, s1.Error)
	So(s2.Warning, ShouldEqual, s1.Warning)
	So(s2.Hidden, ShouldEqual, s1.Hide)
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

func checkFilesIdentical(t *testing.T, files1 []string, files2 []*db.File) {
	t.Helper()

	So(files2, ShouldHaveLength, len(files1))

	for _, file := range files2 {
		So(files1, ShouldContain, file.LocalPath)
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

func removeValue[T comparable](slice []T, value T) []T {
	result := slice[:0]
	for _, v := range slice {
		if v != value {
			result = append(result, v)
		}
	}
	return result
}

// Difference returns a slice containing all elements of slice1 that are not present in slice2.
func Difference[T comparable](slice1, slice2 []T) []T {
	lookup := make(map[T]struct{})
	for _, v := range slice2 {
		lookup[v] = struct{}{}
	}

	var diff []T
	for _, v := range slice1 {
		if _, found := lookup[v]; !found {
			diff = append(diff, v)
		}
	}

	return diff
}

func setRandomFileProperties(t *testing.T, boltDB *set.DB, s *set.Set, files []string) {
	t.Helper()

	entryTypes := []set.EntryType{
		set.Regular, set.Hardlink, set.Symlink, set.Abnormal, set.Unknown,
	}

	entryStatuses := []set.EntryStatus{
		set.Uploaded, set.Failed, set.Replaced, set.Skipped, set.Orphaned,
	}

	for _, file := range files {

		entry, err := boltDB.GetFileEntryForSet(s.ID(), file)
		So(err, ShouldBeNil)

		entry.Type = randomChoice(entryTypes...)

		switch entry.Type {
		case set.Abnormal:
			entry.Status = set.AbnormalEntry
		case set.Unknown:
			entry.Status = set.Missing
		default:
			entry.Status = randomChoice(entryStatuses...)
		}

		entry.Size = uint64(rand.Intn(MB))
		entry.Inode = uint64(rand.Int31())

		err = boltDB.UpdateEntry(s.ID(), file, entry)
		So(err, ShouldBeNil)
	}
}
