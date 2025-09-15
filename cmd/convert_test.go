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
				{
					name:    "You can transfer set Transformer [prefix]",
					setInit: func(s *set.Set) { s.Transformer = "prefix=/a/b/c:/d/e/f" },
					setCheck: func(s *db.Set) {
						So(s.Transformer.Name(), ShouldEqual, "prefix=/a/b/c:/d/e/f")
						So(s.Transformer.Match("/a/b/c/file"), ShouldBeTrue)
					},
				},
				{
					name:     "You can transfer set Description",
					setInit:  func(s *set.Set) { s.Description = "description" },
					setCheck: func(s *db.Set) { So(s.Description, ShouldEqual, "description") },
				},
				{
					name:     "You can transfer set Review date",
					setInit:  func(s *set.Set) { s.Metadata[transfer.MetaKeyReview] = "2025-01-01T15:04:05Z" },
					setCheck: func(s *db.Set) { So(s.ReviewDate, ShouldResemble, time.Date(2025, 1, 1, 15, 4, 5, 0, time.UTC)) },
				},
				{
					name:     "You can transfer set Removal date",
					setInit:  func(s *set.Set) { s.Metadata[transfer.MetaKeyRemoval] = "2026-01-01T15:04:05Z" },
					setCheck: func(s *db.Set) { So(s.DeleteDate, ShouldResemble, time.Date(2026, 1, 1, 15, 4, 5, 0, time.UTC)) },
				},
				{
					name:     "You can transfer set Metadata",
					setInit:  func(s *set.Set) { s.Metadata["a"] = "b" },
					setCheck: func(s *db.Set) { So(s.Metadata, ShouldEqual, db.Metadata{"a": "b"}) },
				},
				{
					name:     "You can transfer set DeleteLocal value",
					setInit:  func(s *set.Set) { s.DeleteLocal = true },
					setCheck: func(s *db.Set) { So(s.DeleteLocal, ShouldEqual, true) },
				},
				{
					name:     "You can transfer set Hide value",
					setInit:  func(s *set.Set) { s.Hide = true },
					setCheck: func(s *db.Set) { So(s.Hidden, ShouldEqual, true) },
				},
				{
					name:     "You can transfer set Warning",
					setInit:  func(s *set.Set) { s.Warning = "warning" },
					setCheck: func(s *db.Set) { So(s.Warning, ShouldEqual, "warning") },
				},
				{
					name:     "You can transfer set Error",
					setInit:  func(s *set.Set) { s.Error = "error" },
					setCheck: func(s *db.Set) { So(s.Error, ShouldEqual, "error") },
				},
			}

			// ReadOnly is transferred within transferAllDataForSet()

			// These values require files in a set (i.e. set during file transfer):
			// StartedDiscovery, LastDiscovery, file counts, Status

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

			for _, b := range []bool{true, false} {
				testCases = append(testCases, testCase{
					name:     fmt.Sprintf("You can transfer set MonitorRemovals [%t]", b),
					setInit:  func(s *set.Set) { s.MonitorRemovals = b },
					setCheck: func(s *db.Set) { So(s.MonitorRemovals, ShouldEqual, b) },
				})
			}

			for r, rc := range map[string]db.Reason{"backup": db.Backup, "archive": db.Archive, "quarantine": db.Quarantine} {
				testCases = append(testCases, testCase{
					name:     fmt.Sprintf("You can transfer set Reason [%s]", r),
					setInit:  func(s *set.Set) { s.Metadata[transfer.MetaKeyReason] = r },
					setCheck: func(s *db.Set) { So(s.Reason, ShouldEqual, rc) },
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
