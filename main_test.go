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
	"path/filepath"
	"strconv"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
	"github.com/wtsi-hgi/ibackup-devkit/cmd"
	"github.com/wtsi-hgi/ibackup/set"
)

func TestMain(t *testing.T) {
	Convey("You cannot run a tool without --database flag", t, func() {
		cmdErrs := new(bytes.Buffer)
		cmd.RootCmd.SetErr(cmdErrs)
		cmd.RootCmd.SetOut(cmdErrs)

		err := cmd.RootCmd.Execute()
		So(err, ShouldNotBeNil)
		So(err.Error(), ShouldContainSubstring, "required flag(s) \"database\" not set")
	})

	Convey("Given a test database file", t, func() {
		testDBFile := filepath.Join(t.TempDir(), "test.db")
		db, err := set.New(testDBFile, "", false)
		So(err, ShouldBeNil)
		for i := range 5 {
			setName := "set-" + strconv.Itoa(i)
			s := &set.Set{
				Name:     setName,
				ReadOnly: i%2 == 0,
			}
			db.AddOrUpdate(s)
		}

		err = db.Close()
		So(err, ShouldBeNil)

		Convey("You can run a tool with --database flag", func() {
			cmd.RootCmd.SetArgs([]string{"--database", testDBFile})

			err := cmd.RootCmd.Execute()
			So(err, ShouldBeNil)

			Convey("And all sets will be read-only", func() {
				db, err := set.New(testDBFile, "", false)
				So(err, ShouldBeNil)

				allSets, err := db.GetAll()
				So(err, ShouldBeNil)

				for _, s := range allSets {
					So(s.ReadOnly, ShouldBeTrue)
				}

				err = db.Close()
				So(err, ShouldBeNil)
			})
		})
	})
}
