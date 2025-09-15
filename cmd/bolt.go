package cmd

import (
	"os"

	"github.com/spf13/cobra"
	"github.com/wtsi-hgi/ibackup/set"
)

var boltCmd = &cobra.Command{
	Use:   "bolt",
	Short: "Update records in the bolt database",
	Long: `Update records in the bolt database

It carries out alterations to all sets in the database.`,
	RunE: func(cmd *cobra.Command, _ []string) error {
		dbPath, err := cmd.Flags().GetString("database")
		if err != nil {
			return err
		}

		makeReadOnly, err := cmd.Flags().GetBool("lock-all-sets")
		if err != nil {
			return err
		}

		hideReadOnly, err := cmd.Flags().GetBool("hide-readonly")
		if err != nil {
			return err
		}

		sets, db, err := getAllSets(dbPath)
		if err != nil {
			return err
		}

		if makeReadOnly {
			makeSetsReadOnly(db, sets)
		}

		if hideReadOnly {
			makeReadOnlySetsHidden(db, sets)
		}

		return db.Close()
	},
}

func init() {
	boltCmd.Flags().String("database", "", "path to the ibackup database file")
	boltCmd.Flags().Bool("lock-all-sets", false, "make all sets in the database read-only")
	boltCmd.Flags().Bool("hide-readonly", false, "make all read-only sets in the database hidden")

	err := boltCmd.MarkFlagRequired("database")
	if err != nil {
		logger.Error(err.Error())
		os.Exit(1)
	}

	RootCmd.AddCommand(boltCmd)
}

func getAllSets(dbPath string) ([]*set.Set, *set.DB, error) {
	_, err := os.Stat(dbPath)
	if err != nil {
		return nil, nil, err
	}

	db, err := set.New(dbPath, "", false)
	if err != nil {
		return nil, nil, err
	}

	allSets, err := db.GetAll()
	if err != nil {
		return nil, nil, err
	}

	return allSets, db, nil
}

func makeSetsReadOnly(db *set.DB, allSets []*set.Set) {
	logStarting(allSets, "read-only")

	for _, s := range allSets {
		if !makeBoolTrue(&s.ReadOnly, s) {
			continue
		}

		err := db.AddOrUpdate(s)
		if err != nil {
			logUpdateFailure(s, err)

			continue
		}

		logUpdateSuccess(s)
	}
}

func logStarting(allSets []*set.Set, propertyName string) {
	logger.Info("updating sets...", "num", len(allSets), "property", propertyName)
}

func makeBoolTrue(b *bool, s *set.Set) bool {
	if *b {
		logSkippingSet(s, "already done")

		return false
	}

	*b = true

	return true
}

func logSkippingSet(s *set.Set, reason string) {
	logger.Info("skipping set", "user", s.Requester, "name", s.Name, "id", s.ID(), "reason", reason)
}

func logUpdateFailure(s *set.Set, err error) {
	logger.Error("failed to update set", "user", s.Requester, "name", s.Name, "id", s.ID(), "err", err)
}

func logUpdateSuccess(s *set.Set) {
	logger.Info("updated set", "user", s.Requester, "name", s.Name, "id", s.ID())
}

func makeReadOnlySetsHidden(db *set.DB, allSets []*set.Set) {
	logStarting(allSets, "hidden")

	for _, s := range allSets {
		if !s.ReadOnly {
			logSkippingSet(s, "not read-only")

			continue
		}

		if !makeBoolTrue(&s.Hide, s) {
			continue
		}

		err := db.Hide(s)
		if err != nil {
			logUpdateFailure(s, err)

			continue
		}

		logUpdateSuccess(s)
	}
}
