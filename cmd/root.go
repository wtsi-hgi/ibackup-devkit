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

// package cmd is the cobra file that enables subcommands and handles
// command-line args.

package cmd

import (
	"os"

	"github.com/inconshreveable/log15"
	"github.com/spf13/cobra"
	"github.com/wtsi-hgi/ibackup/set"
)

// logger is used for logging events in our commands.
var logger = log15.New()

// RootCmd represents the base command when called without any subcommands.
var RootCmd = &cobra.Command{
	Use:   "ibackup-devkit",
	Short: "Toolkit to work with ibackup database",
	Long: `ibackup's separate database-altering utility.
	
It carries out alterations to all sets in the database.`,

	PersistentPreRun: func(cmd *cobra.Command, _ []string) {
		// set up logging to stdout
		logHandler := log15.StreamHandler(cmd.OutOrStdout(), log15.TerminalFormat())
		logger.SetHandler(log15.LvlFilterHandler(log15.LvlInfo, logHandler))
	},

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

// Execute adds all child commands to the root command and sets flags
// appropriately. This is called by main.main(). It only needs to happen once to
// the rootCmd.
func Execute() {
	if err := RootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}

func init() {
	RootCmd.Flags().String("database", "", "path to the ibackup database file")
	RootCmd.Flags().Bool("lock-all-sets", false, "make all sets in the database read-only")
	RootCmd.Flags().Bool("hide-readonly", false, "make all read-only sets in the database hidden")

	err := RootCmd.MarkFlagRequired("database")
	if err != nil {
		logger.Error(err.Error())
		os.Exit(1)
	}
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
	updateSetsProperty(db, allSets, "read-only", func(*set.Set) bool {
		return false
	}, func(s *set.Set) *bool {
		return &s.ReadOnly
	})
}

func updateSetsProperty(db *set.DB, allSets []*set.Set, propertyName string,
	skipSet func(*set.Set) bool, propertySelector func(*set.Set) *bool) {
	logger.Info("updating sets...", "num", len(allSets), "property", propertyName)

	for _, s := range allSets {
		if skipSet(s) {
			logger.Info("skipping set (filtered)", "user", s.Requester, "name", s.Name, "id", s.ID())

			continue
		}

		property := propertySelector(s)
		if *property {
			logger.Info("skipping set (already done)", "user", s.Requester, "name", s.Name, "id", s.ID())

			continue
		}

		*property = true

		err := db.AddOrUpdate(s)
		if err != nil {
			logger.Error("failed to update set", "user", s.Requester, "name", s.Name, "id", s.ID(), "err", err)

			continue
		}

		logger.Info("updated set", "user", s.Requester, "name", s.Name, "id", s.ID())
	}
}

func makeReadOnlySetsHidden(db *set.DB, allSets []*set.Set) {
	updateSetsProperty(db, allSets, "hidden", func(s *set.Set) bool {
		return !s.ReadOnly
	}, func(s *set.Set) *bool {
		return &s.Hide
	})
}
