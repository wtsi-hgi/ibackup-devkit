package cmd

import (
	"fmt"
	"github.com/wtsi-hgi/ibackup/set"
	"github.com/wtsi-hgi/ibackup/transfer"
	"testing"
	"time"

	"github.com/wtsi-hgi/ibackup/db"

	. "github.com/smartystreets/goconvey/convey"
	_ "modernc.org/sqlite"
)

func TestTransferSet(t *testing.T) {
	Convey("With a test sql connection", t, func() {
		sqlitePath := t.TempDir() + "/test.db"

		sqlDB, err := db.Init("sqlite", sqlitePath)
		So(err, ShouldBeNil)

		t.Cleanup(func() {
			callAndLogTestError(t, sqlDB.Close)
		})

		Convey("And a minimal set", func() {
			testSet := &set.Set{
				Name:        "testSet",
				Requester:   "testUser",
				Transformer: "humgen",
				Metadata: map[string]string{
					transfer.MetaKeyReason:  "backup",
					transfer.MetaKeyReview:  "2006-01-02T15:04:05Z",
					transfer.MetaKeyRemoval: "2007-01-02T15:04:05Z",
				},
			}

			type testCase struct {
				name     string
				setInit  func(set *set.Set)
				setCheck func(set *db.Set)
			}

			testCases := []testCase{
				{
					name:     "You can transfer set Name",
					setInit:  func(s *set.Set) { s.Name = "setName" },
					setCheck: func(s *db.Set) { So(s.Name, ShouldEqual, "setName") },
				},
				{
					name:     "You can transfer set Requester",
					setInit:  func(s *set.Set) { s.Requester = "requesterName" },
					setCheck: func(s *db.Set) { So(s.Requester, ShouldEqual, "requesterName") },
				},
				{
					name:     "You can transfer set MonitorTime",
					setInit:  func(s *set.Set) { s.MonitorTime = 24 * time.Hour },
					setCheck: func(s *db.Set) { So(s.MonitorTime, ShouldEqual, 24*time.Hour/time.Second) },
				},
			}

			for _, trans := range []string{"humgen", "gengen", "otar"} {
				testCases = append(testCases, testCase{
					name:    fmt.Sprintf("You can transfer set Transformer [%s]", trans),
					setInit: func(s *set.Set) { s.Transformer = trans },
					setCheck: func(s *db.Set) {
						So(s.Transformer.Name(), ShouldEqual, trans)
						So(s.Transformer.Match(fmt.Sprintf("/lustre/scratch123/%s/projects_v2/project/file", trans)), ShouldBeTrue)
					},
				})
			}

			testCases = append(testCases, testCase{
				name:    "You can transfer set Transformer [prefix]",
				setInit: func(s *set.Set) { s.Transformer = "prefix=/a/b/c:/d/e/f" },
				setCheck: func(s *db.Set) {
					So(s.Transformer.Name(), ShouldEqual, "prefix=/a/b/c:/d/e/f")
					So(s.Transformer.Match("/a/b/c/file"), ShouldBeTrue)
				},
			})

			for _, b := range []bool{true, false} {
				testCases = append(testCases, testCase{
					name:     fmt.Sprintf("You can transfer set MonitorRemovals [%t]", b),
					setInit:  func(s *set.Set) { s.MonitorRemovals = b },
					setCheck: func(s *db.Set) { So(s.MonitorRemovals, ShouldEqual, b) },
				})
			}

			for _, tc := range testCases {
				Convey(tc.name, func() {
					tc.setInit(testSet)

					_, err = transferSet(sqlDB, testSet)
					So(err, ShouldBeNil)

					resultSet, err := sqlDB.GetSet(testSet.Name, testSet.Requester)
					So(err, ShouldBeNil)

					tc.setCheck(resultSet)
				})
			}
		})
	})
}

func callAndLogTestError(t *testing.T, f func() error) {
	t.Helper()

	err := f()
	if err != nil {
		t.Log(err)
	}
}
