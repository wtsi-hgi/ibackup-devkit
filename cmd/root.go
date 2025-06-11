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
	"fmt"

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
	
	Currently, it can only make all sets read-only.`,
	RunE: func(cmd *cobra.Command, args []string) error {
		db, err := cmd.Flags().GetString("database")
		if err != nil {
			return err
		}

		return updateDatabase(db)
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
	// set up logging to stdout
	logger.SetHandler(log15.LvlFilterHandler(log15.LvlInfo, log15.StdoutHandler))

	// global flags
	RootCmd.Flags().String("database", "", "path to the ibackup database file")

	err := RootCmd.MarkFlagRequired("database")
	if err != nil {
		logger.Error(err.Error())
		os.Exit(1)
	}
}

func updateDatabase(dbPath string) error {
	db, err := set.New(dbPath, "", false)
	if err != nil {
		return err
	}

	allSets, err := db.GetAll()
	if err != nil {
		return err
	}

	logger.Info(fmt.Sprintf("Updating %d sets to read-only mode...", + len(allSets)))

	for _, s := range allSets {
		logger.Info("Updating set", "name", s.Name, "id", s.ID())

		if s.ReadOnly {
			logger.Info("Set is already read-only, skipping...")
			continue
		}

		s.ReadOnly = true
		err = db.AddOrUpdate(s)
		if err != nil {
			logger.Error("Failed to update set. Skipping...")
		} else {
			logger.Info("Set updated successfully")
		}
	}

	return db.Close()
}
