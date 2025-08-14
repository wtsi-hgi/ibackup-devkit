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

func TestConvert(t *testing.T) {
	Convey("Given a test bolt database", t, func() {
		testBoltFile := filepath.Join(t.TempDir(), "test.db")
		boltDB, err := set.New(testBoltFile, "", false)
		So(err, ShouldBeNil)

		testSets := generateRandomSets(5)

		for _, s := range testSets {
			err = boltDB.AddOrUpdate(s)
			So(err, ShouldBeNil)
		}

		err = boltDB.Close()
		So(err, ShouldBeNil)

		cmdErrs := new(bytes.Buffer)
		cmd.RootCmd.SetOut(cmdErrs)

		Convey("And a connection to a MySQL database", func() {
			url, err := cmd.BuildSQLURL()
			So(err, ShouldBeNil)

			sqlDB, err := db.Init("mysql", url)
			So(err, ShouldBeNil)
			So(sqlDB, ShouldNotBeNil)
			//})
			//
			//SkipConvey("And a test SQLite database", func() {
			//	testBoltFile = filepath.Join(t.TempDir(), "db?journal_mode=WAL")
			//	sqlDB, err := db.Init("sqlite", testBoltFile)
			//	So(err, ShouldBeNil)

			defer func() {
				err = sqlDB.Close()
				if err != nil {
					t.Log(err)
				}
			}()

			resetDatabase(t, sqlDB)

			Convey("You can transfer sets", func() {
				cmd.RootCmd.SetArgs([]string{"convert", "--bolt", testBoltFile})

				err := cmd.RootCmd.Execute()
				So(err, ShouldBeNil)

				for _, s := range testSets {
					newSet, err := sqlDB.GetSet(s.Name, s.Requester)
					So(err, ShouldBeNil)

					areSetsIdentical(t, s, newSet)
				}
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

func resetDatabase(t *testing.T, sqlDB *db.DB) {
	t.Helper()

	sets := sqlDB.GetAllSets()

	err := sets.ForEach(func(s *db.Set) error {
		err := sqlDB.SetSetModifiable(s)
		if err != nil {
			return err
		}

		return sqlDB.DeleteSet(s)
	})

	So(err, ShouldBeNil)
}

func areSetsIdentical(t *testing.T, s1 *set.Set, s2 *db.Set) {
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
	So(s2.LastDiscovery, ShouldEqual, s1.LastDiscovery)
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
