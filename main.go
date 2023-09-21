// The following enables go generate to generate the doc.go file.
//go:generate go run v.io/x/lib/cmdline/gendoc "--build-cmd=go install" --copyright-notice= .
package main

import (
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"strings"
	"time"

	_ "github.com/grailbio/v23/factories/grail"
	"grail.com/tidy/dataframe"
	tidy "grail.com/tidy/vanadium/client"
	vdl "grail.com/tidy/vanadium/vdl/dataset"
	v23 "v.io/v23"
	"v.io/v23/context"
	"v.io/x/lib/cmdline"
	"v.io/x/ref/lib/v23cmd"
)

var (
	addressFlag         string
	filtersFlag         string
	materializeFlag     string
	outputFlag          string
	publishStateStrFlag string
	identityFlag        string
	verboseFlag         bool
	withAliasesFlag     bool
	client              tidy.Client
)

func cmdRoot() *cmdline.Command {
	root := &cmdline.Command{
		Name:  "tidydata-client",
		Short: "Query datasets from tidyservice server.",
		Long: `
		Queries datasets from tidyservice vanadium server.'
		`,
		LookPath: false,
		Children: []*cmdline.Command{
			cmdTidyset(),
			cmdVersion(),
			cmdList(),
			cmdDescribe(),
			cmdCheckAccess(),
			cmdPreprocessed(),
			cmdReleaseNotes(),
		},
		Topics: []cmdline.Topic{},
	}
	root.Flags.StringVar(&addressFlag, "address", "tidy/prod/dataset", "The vanadium endpoint to communicate with.")
	return root
}

// Makes sure there are the right number of arguments. Requires dataset, version, and tableset. Certain tablesets require filters.
func parseTidyArgs(args []string) error {
	if len(args) != 3 {
		return errors.New("need exactly 3 arguments: <dataset> <version> <tableset>")
	}
	return nil
}

func outputPathAndVersion(env *cmdline.Env, path, version string) error {
	output := strings.Join([]string{path, version}, "\n")
	_, err := env.Stdout.Write([]byte(output + "\n"))
	return err
}

func runTidyset(ctx *context.T, env *cmdline.Env, args []string) error {
	client = tidy.NewTidyClient(ctx, addressFlag)
	if err := parseTidyArgs(args); err != nil {
		return err
	}
	var filters []string
	if len(filtersFlag) > 0 {
		filters = strings.Split(filtersFlag, ",")
	}
	var filtersToMaterialize []string
	if len(materializeFlag) > 0 {
		filtersToMaterialize = strings.Split(materializeFlag, ",")
	}

	if args[0] == "client-test" {
		return loadTestData(env)
	}

	path, version, err := client.GetData(args[0], args[1], args[2], filters, filtersToMaterialize)
	if err != nil {
		return err
	}
	if outputFlag != "" {
		data, err := ioutil.ReadFile(path)
		if err != nil {
			return err
		}
		if err = ioutil.WriteFile(outputFlag, data, os.ModePerm); err != nil {
			return err
		}
	}
	return outputPathAndVersion(env, path, version)
}

func cmdReleaseNotes() *cmdline.Command {
	cmd := &cmdline.Command{
		Runner:   v23cmd.RunnerFunc(runReleaseNotes),
		Name:     "release-notes",
		Short:    "Returns URL to release notes for given dataset and version",
		Long:     "Returns URL to release notes for given dataset and version",
		ArgsName: "[--filters filters] <dataset> <version>",
	}
	return cmd
}

func runReleaseNotes(ctx *context.T, env *cmdline.Env, args []string) error {
	client = tidy.NewTidyClient(ctx, addressFlag)
	_, err := client.GetVersionFor(args[1], args[0])
	if err != nil {
		return err
	}
	if len(args) != 2 {
		return errors.New("need exactly 2 arguments: <dataset> <version>")
	}
	fmt.Printf("https://confluence.ti-apps.aws.grail.com/display/TIDY/Release+%s-%s-Tidydata+Software+Release+Notes", args[1], args[0])
	return nil
}

func runCheckAccess(ctx *context.T, env *cmdline.Env, args []string) error {
	client = tidy.NewTidyClient(ctx, addressFlag)
	if err := parseTidyArgs(args); err != nil {
		return err
	}
	var filters []string
	if len(filtersFlag) > 0 {
		filters = strings.Split(filtersFlag, ",")
	}
	blessings, _ := v23.GetPrincipal(ctx).BlessingStore().Default()
	identity := blessings.String()
	if len(identityFlag) > 0 {
		identity = identityFlag
	}

	if err := client.CheckAccess(identity, args[0], args[1], args[2], filters); err != nil {
		return err
	}
	fmt.Printf("%s has access to %s %s %s\n", identity, args[0], args[1], args[2])
	return nil
}

func cmdCheckAccess() *cmdline.Command {
	cmd := &cmdline.Command{
		Runner:   v23cmd.RunnerFunc(runCheckAccess),
		Name:     "check-access",
		Short:    "Checks if a v23 identity has access to a certain request",
		Long:     "Checks if a v23 identity has access to a certain request",
		ArgsName: "[--filters filters] [--identity identity] <dataset> <version> <tableset>",
	}
	cmd.Flags.StringVar(&filtersFlag, "filters", "", "Filters to use in a comma-separated string.")
	cmd.Flags.StringVar(&identityFlag, "identity", "", "identity string to be used to check access, if not the user making the request")
	return cmd
}

func loadTestData(env *cmdline.Env) error {
	// load the asset from bindata
	b, err := Asset("go/src/grail.com/cmd/tidydata-client/testdata/tidydata.db")
	if err != nil {
		return err
	}
	err = os.MkdirAll("/tmp/grail-cache/.grail-tidydata", 0777)
	if err != nil {
		return err
	}
	path := "/tmp/grail-cache/.grail-tidydata/tidydata_test.db"
	t := time.Now()
	dat := t.Format("2006-01-02-15-04-05")
	vers := fmt.Sprintf("%v", dat)
	if _, err := os.Stat(path); os.IsNotExist(err) {
		err = ioutil.WriteFile(path, b, 0644)
		if err != nil {
			return err
		}
	}
	return outputPathAndVersion(env, path, vers)
}

func cmdTidyset() *cmdline.Command {
	cmd := &cmdline.Command{
		Runner:   v23cmd.RunnerFunc(runTidyset),
		Name:     "tidyset",
		Short:    "Queries for the Tidyset based on the dataset, version, tableset, and filters.",
		Long:     "Queries for the Tidyset based on the dataset, version, tableset, and filters.",
		ArgsName: "[--filters filters] <dataset> <version> <tableset>",
	}
	cmd.Flags.StringVar(&filtersFlag, "filters", "", "Filters to use in a comma-separated string.")
	cmd.Flags.StringVar(&materializeFlag, "materialize", "", "Filters to materialize in a comma-separated string.")
	cmd.Flags.StringVar(&outputFlag, "o", "", "Path to output dataset. Will write to tidydata cache by default.")
	return cmd
}

func runAddVersion(ctx *context.T, env *cmdline.Env, args []string) error {
	client = tidy.NewTidyClient(ctx, addressFlag)
	if len(args) != 2 {
		return errors.New("need exactly 2 arguments: <dataset> <version>")
	}
	dataset := args[0]
	version := args[1]
	state := vdl.StateGenerating
	return client.AddVersion(dataset, version, state)
}

func cmdAddVersion() *cmdline.Command {
	cmd := &cmdline.Command{
		Runner:   v23cmd.RunnerFunc(runAddVersion),
		Name:     "add",
		Short:    "add a new version",
		Long:     "Add a new version referencing data at s3://<bucket>/<version-key>.",
		ArgsName: "<dataset> <version-key>",
	}
	return cmd
}

func runUpdatePublishState(ctx *context.T, env *cmdline.Env, args []string) error {
	client = tidy.NewTidyClient(ctx, addressFlag)
	if len(args) != 3 {
		return errors.New("need exactly 3 arguments: <dataset> <version> <state>")
	}
	dataset := args[0]
	version := args[1]
	stateStr := args[2]
	state, err := vdl.StateFromString(stateStr)
	if err != nil {
		return fmt.Errorf("couldn't parse state %v: %v", stateStr, err)
	}
	return client.UpdateVersionState(dataset, version, state)
}

func cmdUpdatePublishState() *cmdline.Command {
	cmd := &cmdline.Command{
		Runner:   v23cmd.RunnerFunc(runUpdatePublishState),
		Name:     "update",
		Short:    "update publish state",
		Long:     "Update an existing version. Version for s3://<bucket>/<version-key> must already exist.",
		ArgsName: "<dataset> <version-key> <version state>",
	}
	return cmd
}

func runUpdateDescription(ctx *context.T, env *cmdline.Env, args []string) error {
	client = tidy.NewTidyClient(ctx, addressFlag)
	if len(args) != 3 {
		return errors.New("need exactly 3 arguments: <dataset> <version> <description>")
	}
	dataset := args[0]
	version := args[1]
	desc := args[2]
	return client.UpdateVersionDescription(dataset, version, desc)
}

func cmdUpdateDescription() *cmdline.Command {
	cmd := &cmdline.Command{
		Runner:   v23cmd.RunnerFunc(runUpdateDescription),
		Name:     "update-description",
		Short:    "update dataset description",
		Long:     "Update a dataset description for a given dataset-version",
		ArgsName: "<dataset> <version> <description>",
	}
	return cmd
}

func runRemoveVersionAlias(ctx *context.T, env *cmdline.Env, args []string) error {
	client = tidy.NewTidyClient(ctx, addressFlag)
	if len(args) != 2 {
		return errors.New("need exactly 2 arguments: <dataset> <alias>")
	}
	dataset := args[0]
	alias := args[1]
	return client.RemoveVersionAlias(dataset, alias)
}

func cmdRemoveVersionAlias() *cmdline.Command {
	cmd := &cmdline.Command{
		Runner:   v23cmd.RunnerFunc(runRemoveVersionAlias),
		Name:     "remove-alias",
		Short:    "remove an alias",
		Long:     "Remove an alias.",
		ArgsName: "<dataset> <alias>",
	}
	return cmd
}

func runUpdateVersionAlias(ctx *context.T, env *cmdline.Env, args []string) error {
	client = tidy.NewTidyClient(ctx, addressFlag)
	if len(args) != 3 {
		return errors.New("need exactly 3 arguments: <dataset> <alias> <new_alias>")
	}
	dataset := args[0]
	alias := args[1]
	newAlias := args[2]
	return client.UpdateVersionAlias(dataset, alias, newAlias)
}

func cmdUpdateVersionAlias() *cmdline.Command {
	cmd := &cmdline.Command{
		Runner:   v23cmd.RunnerFunc(runUpdateVersionAlias),
		Name:     "update-alias",
		Short:    "update an alias",
		Long:     "Update an alias to a different name.",
		ArgsName: "<dataset> <alias> <new_alias>",
	}
	return cmd
}

func runAddAlias(ctx *context.T, env *cmdline.Env, args []string) error {
	client = tidy.NewTidyClient(ctx, addressFlag)
	if len(args) != 3 {
		return errors.New("need exactly 3 arguments: <dataset> <version> <alias>")
	}
	dataset := args[0]
	version := args[1]
	alias := args[2]
	return client.AddVersionAlias(dataset, version, alias)
}

func cmdAddAlias() *cmdline.Command {
	cmd := &cmdline.Command{
		Runner:   v23cmd.RunnerFunc(runAddAlias),
		Name:     "add-alias",
		Short:    "add an alias",
		Long:     "Add an alias. Links to data references data at s3://<bucket>/<version>.",
		ArgsName: "<dataset> <version-key> <alias>",
	}
	return cmd
}

func cmdVersion() *cmdline.Command {
	cmd := &cmdline.Command{
		Name:  "version",
		Short: "Publish and fail versions. Admin only.",
		Long:  "Publish and fail versions. Admin only,",
		Children: []*cmdline.Command{
			cmdAddVersion(),
			cmdUpdatePublishState(),
			cmdAddAlias(),
			cmdUpdateDescription(),
			cmdRemoveVersionAlias(),
			cmdUpdateVersionAlias(),
		},
	}
	cmd.Flags.StringVar(&filtersFlag, "filters", "", "Filters to use in a comma-separated string.")
	return cmd
}

func runListDatasets(ctx *context.T, env *cmdline.Env, args []string) error {
	client = tidy.NewTidyClient(ctx, addressFlag)
	datasets, err := client.ListDatasets()
	if err == nil {
		fmt.Println("Datasets:")
		fmt.Println(strings.Join(datasets, "\n"))
	}
	return err
}

func cmdListDatasets() *cmdline.Command {
	cmd := &cmdline.Command{
		Name:   "datasets",
		Short:  "lists all available datasets",
		Long:   "lists all available datasets",
		Runner: v23cmd.RunnerFunc(runListDatasets),
	}
	return cmd
}

func runListVersions(ctx *context.T, env *cmdline.Env, args []string) error {
	client = tidy.NewTidyClient(ctx, addressFlag)
	if len(args) != 1 {
		return errors.New("need exactly 1 argument: <dataset>")
	}
	dataset := args[0]
	state, err := vdl.StateFromString(publishStateStrFlag)
	if err != nil {
		return fmt.Errorf("couldn't parse publish state %v: %v", publishStateStrFlag, err)
	}
	var versionStrings, aliasStrings, stateStrings, descriptionStrings []string
	if withAliasesFlag {
		versions, err := client.ListAliasedVersions(dataset)
		if err != nil {
			return err
		}
		for _, v := range versions {
			if v.State >= state {
				versionStrings = append(versionStrings, v.Version)
				aliasStrings = append(aliasStrings, v.Alias)
				stateStrings = append(stateStrings, v.State.String())
				descriptionStrings = append(descriptionStrings, v.Description)
			}
		}
		df := dataframe.New("aliased_versions",
			dataframe.StringSeries(aliasStrings, "alias"),
			dataframe.StringSeries(versionStrings, "version"),
			dataframe.StringSeries(stateStrings, "publish_state"),
			dataframe.StringSeries(descriptionStrings, "description"))
		fmt.Println(df)
	} else {
		versions, err := client.ListVersionsAt(dataset, state)
		if err != nil {
			return err
		}
		for _, v := range versions {
			versionStrings = append(versionStrings, v.Version)
			stateStrings = append(stateStrings, v.State.String())
		}
		df := dataframe.New("versions",
			dataframe.StringSeries(versionStrings, "version"),
			dataframe.StringSeries(stateStrings, "publish_state"))
		_ = df.Sort(dataframe.ColSort{
			"version",
			false,
			false,
		})
		fmt.Println("versions:")
		for i := 0; i < df.NumRows(); i++ {
			fmt.Printf("%v: %v \n", df.MustAt(i, "version").String(), df.MustAt(i, "publish_state").String())
		}
	}
	return nil
}

func cmdListVersions() *cmdline.Command {
	cmd := &cmdline.Command{
		Name:     "versions",
		Short:    "lists available versions",
		Long:     "lists available versions",
		ArgsName: "<dataset>",
		Runner:   v23cmd.RunnerFunc(runListVersions),
	}
	cmd.Flags.BoolVar(&withAliasesFlag, "with_alias", true, "only show versions with aliases")
	cmd.Flags.StringVar(&publishStateStrFlag, "publish_state", "tested", "minimum publish state")

	return cmd
}

func runListTablesets(ctx *context.T, env *cmdline.Env, args []string) error {
	client = tidy.NewTidyClient(ctx, addressFlag)
	if len(args) != 2 {
		return errors.New("need exactly 2 arguments: <dataset> <version>")
	}
	dataset := args[0]
	version := args[1]
	tablesets, err := client.ListTablesets(dataset, version)
	if err == nil {
		fmt.Println("Tablesets:")
		fmt.Println(strings.Join(tablesets, "\n"))
	}
	return err
}

func cmdListTablesets() *cmdline.Command {
	cmd := &cmdline.Command{
		Name:     "tablesets",
		Short:    "lists all available tablesets",
		Long:     "lists all available tablesets",
		ArgsName: "<dataset> <version>",
		Runner:   v23cmd.RunnerFunc(runListTablesets),
	}
	return cmd
}

func runListFilters(ctx *context.T, env *cmdline.Env, args []string) error {
	client = tidy.NewTidyClient(ctx, addressFlag)
	if len(args) != 2 {
		return errors.New("need exactly 2 arguments: <dataset> <version>")
	}
	dataset := args[0]
	version := args[1]
	filters, err := client.ListFilters(dataset, version)
	if err != nil {
		return err
	}
	fmt.Println("Name")
	for _, f := range filters {
		fmt.Println(f)
	}
	return nil
}

func cmdListFilters() *cmdline.Command {
	cmd := &cmdline.Command{
		Name:     "filters",
		Short:    "lists all available filters",
		Long:     "lists all available filters",
		ArgsName: "<dataset> <version>",
		Runner:   v23cmd.RunnerFunc(runListFilters),
	}
	return cmd
}

func cmdListTables() *cmdline.Command {
	cmd := &cmdline.Command{
		Name:     "tables",
		Short:    "lists included tables",
		Long:     "lists included tables in a tableset for a dataset-version",
		ArgsName: "<dataset> <version> <tableset>",
		Runner:   v23cmd.RunnerFunc(runListTables),
	}
	return cmd
}

func runListTables(ctx *context.T, env *cmdline.Env, args []string) error {
	client = tidy.NewTidyClient(ctx, addressFlag)
	if len(args) != 3 {
		return errors.New("need exactly 3 arguments: <dataset> <version> <tableset>")
	}
	dataset := args[0]
	version := args[1]
	tableset := args[2]
	tables, err := client.ListTables(dataset, version, tableset)
	if err != nil {
		return err
	}
	fmt.Println("Included tables:")
	for _, t := range tables {
		fmt.Println(t)
	}
	return nil
}

func cmdListAliases() *cmdline.Command {
	cmd := &cmdline.Command{
		Name:     "aliases",
		Short:    "list aliases for a given dataset",
		Long:     "list all aliases for a given dataset",
		ArgsName: "<dataset> <version>",
		Runner:   v23cmd.RunnerFunc(runListAliases),
	}
	return cmd
}

func runListAliases(ctx *context.T, env *cmdline.Env, args []string) error {
	client = tidy.NewTidyClient(ctx, addressFlag)
	if len(args) != 2 {
		return fmt.Errorf("need exactly two arguments: <dataset> <version>")
	}
	dataset := args[0]
	version := args[1]
	aliases, err := client.ListAliases(dataset, version)
	if err == nil {
		if len(aliases) == 0 {
			fmt.Printf("No aliases found for dataset %s and version %s\n", dataset, version)
		} else {
			fmt.Printf("Aliases for version %v \n", version)
			for _, a := range aliases {
				fmt.Println(a)
			}
		}
	}
	return err
}

func cmdListSnapshots() *cmdline.Command {
	cmd := &cmdline.Command{
		Name:     "snapshots",
		Short:    "list clinical data snapshots for a given dataset",
		Long:     "list clinical data snapshots for a given dataset",
		ArgsName: "<dataset> <version>",
		Runner:   v23cmd.RunnerFunc(runListSnapshots),
	}
	return cmd
}

func runListSnapshots(ctx *context.T, env *cmdline.Env, args []string) error {
	client = tidy.NewTidyClient(ctx, addressFlag)
	if len(args) != 2 {
		return fmt.Errorf("need exactly two arguments: <dataset> <version>")
	}
	dataset := args[0]
	version := args[1]
	snapshots, err := client.ListSnapshots(dataset, version)
	if err == nil {
		for _, s := range snapshots {
			fmt.Println(s)
		}
	}
	return err
}

func cmdList() *cmdline.Command {
	cmd := &cmdline.Command{
		Name:  "list",
		Short: "lists all available datasets, versions, filters, and tablesets",
		Long:  "lists all available datasets, versions, filters, and tablesets",
		Children: []*cmdline.Command{
			cmdListDatasets(),
			cmdListVersions(),
			cmdListTablesets(),
			cmdListFilters(),
			cmdListAliases(),
			cmdListTables(),
			cmdListSnapshots(),
		},
	}
	cmd.Flags.BoolVar(&verboseFlag, "verbose", false, "List available values with their definitions.")
	return cmd
}

func runDescribeDataset(ctx *context.T, env *cmdline.Env, args []string) error {
	client = tidy.NewTidyClient(ctx, addressFlag)
	if len(args) != 1 {
		return errors.New("need exactly 1 argument: <dataset>")
	}
	dataset := args[0]
	description, err := client.DescribeDataset(dataset)
	if err == nil {
		fmt.Println(description)
	}
	return err
}

func cmdDescribeDataset() *cmdline.Command {
	cmd := &cmdline.Command{
		Name:     "dataset",
		Short:    "describes a dataset",
		Long:     "describes a dataset",
		ArgsName: "<dataset>",
		Runner:   v23cmd.RunnerFunc(runDescribeDataset),
	}
	return cmd
}

func runDescribeVersion(ctx *context.T, env *cmdline.Env, args []string) error {
	client = tidy.NewTidyClient(ctx, addressFlag)
	if len(args) != 2 {
		return errors.New("need exactly 2 arguments: <dataset> <version>")
	}
	dataset := args[0]
	version := args[1]
	description, err := client.DescribeVersion(dataset, version)
	if err == nil {
		fmt.Println(description)
	}
	return err
}

func cmdDescribeVersion() *cmdline.Command {
	cmd := &cmdline.Command{
		Name:     "version",
		Short:    "describes a version of a dataset",
		Long:     "describes a version of a dataset",
		ArgsName: "<dataset> <version>",
		Runner:   v23cmd.RunnerFunc(runDescribeVersion),
	}
	return cmd
}

func runDescribeTableset(ctx *context.T, env *cmdline.Env, args []string) error {
	client = tidy.NewTidyClient(ctx, addressFlag)
	if len(args) != 3 {
		return errors.New("need exactly 3 arguments: <dataset> <version> <tableset>")
	}
	dataset := args[0]
	version := args[1]
	tableset := args[2]
	description, err := client.DescribeTableset(dataset, version, tableset)
	if err == nil {
		fmt.Println(description)
	}
	return err
}

func cmdDescribeTableset() *cmdline.Command {
	cmd := &cmdline.Command{
		Name:     "tableset",
		Short:    "describe a tableset for a dataset and version",
		Long:     "describe a tableset for a dataset and version",
		ArgsName: "<dataset> <version> <tableset>",
		Runner:   v23cmd.RunnerFunc(runDescribeTableset),
	}
	return cmd
}

func runDescribeFilter(ctx *context.T, env *cmdline.Env, args []string) error {
	client = tidy.NewTidyClient(ctx, addressFlag)
	if len(args) != 3 {
		return errors.New("need exactly 3 arguments: <dataset> <version> <filter>")
	}
	dataset := args[0]
	version := args[1]
	filter := args[2]
	description, queryString, err := client.DescribeFilter(dataset, version, filter)
	if err == nil {
		fmt.Println("Query String:")
		fmt.Println(queryString)
		fmt.Println("Description:")
		fmt.Println(description)
	}
	return err
}

func cmdDescribeFilter() *cmdline.Command {
	cmd := &cmdline.Command{
		Name:     "filter",
		Short:    "describe a filter for a dataset and version",
		Long:     "describe a filter for a dataset and version",
		ArgsName: "<dataset> <version> <filter>",
		Runner:   v23cmd.RunnerFunc(runDescribeFilter),
	}
	return cmd
}

func runDescribeTable(ctx *context.T, env *cmdline.Env, args []string) error {
	client = tidy.NewTidyClient(ctx, addressFlag)
	if len(args) != 4 {
		return errors.New("need exactly 4 arguments: <dataset> <version> <tableset> <table>")
	}
	dataset := args[0]
	version := args[1]
	tableset := args[2]
	table := args[3]
	info, err := client.DescribeTable(dataset, version, tableset, table)
	if err != nil {
		return err
	}
	fmt.Printf("Table %v in tableset %v: \n", table, tableset)
	fmt.Printf("Number of rows: %v \n", info.NumRows)
	fmt.Println("Columns:")
	for _, c := range info.Columns {
		fmt.Println(c)
	}
	return nil
}

func cmdDescribeTable() *cmdline.Command {
	cmd := &cmdline.Command{
		Name:     "table",
		Short:    "describe a table for a tableset in a dataset and version",
		Long:     "describe a table for a tableset in a dataset and version",
		ArgsName: "<dataset> <version> <tableset> <table>",
		Runner:   v23cmd.RunnerFunc(runDescribeTable),
	}
	return cmd
}

func cmdDescribeColumn() *cmdline.Command {
	cmd := &cmdline.Command{
		Name:     "column",
		Short:    "describe a column for a table in a dataset and version",
		Long:     "describe a column for a table in a dataset and version. Currently only supported for clinical tables.",
		ArgsName: "<dataset> <version> <table> <column>",
		Runner:   v23cmd.RunnerFunc(runDescribeColumn),
	}
	return cmd
}

func runDescribeColumn(ctx *context.T, env *cmdline.Env, args []string) error {
	client = tidy.NewTidyClient(ctx, addressFlag)
	if len(args) != 4 {
		return errors.New("need exactly 4 arguments: <dataset> <version> <table> <column>")
	}
	dataset := args[0]
	version := args[1]
	table := args[2]
	column := args[3]
	description, err := client.DescribeColumn(dataset, version, table, column)
	if err != nil {
		return err
	}
	fmt.Printf("Column %s: \n", column)
	fmt.Printf("Description: %s \n", description[0])
	if description[1] != "" {
		fmt.Println("Rule:")
		fmt.Println(description[1])
	}
	return nil
}

func cmdDescribe() *cmdline.Command {
	cmd := &cmdline.Command{
		Name:  "describe",
		Short: "describes all available datasets, versions, filters, and tablesets",
		Long:  "describes all available datasets, versions, filters, and tablesets",
		Children: []*cmdline.Command{
			cmdDescribeDataset(),
			cmdDescribeVersion(),
			cmdDescribeTableset(),
			cmdDescribeFilter(),
			cmdDescribeTable(),
			cmdDescribeColumn(),
		},
	}
	return cmd
}

func runPreprocessedData(ctx *context.T, env *cmdline.Env, args []string) error {
	client := tidy.NewTidyClient(ctx, addressFlag)
	if len(args) != 2 {
		return errors.New("need exactly 2 arguments: <dataset> <version>")
	}
	path, version, err := client.GetPreprocessedData(args[0], args[1])
	if err != nil {
		return err
	}
	if outputFlag != "" {
		data, err := ioutil.ReadFile(path)
		if err != nil {
			return err
		}
		if err = ioutil.WriteFile(outputFlag, data, os.ModePerm); err != nil {
			return err
		}
	}
	return outputPathAndVersion(env, path, version)
}

func cmdPreprocessed() *cmdline.Command {
	cmd := &cmdline.Command{
		Name:     "preprocessed-data",
		Short:    "Fetches preprocessed data for given dataset and version",
		Long:     "Fetches preprocessed data for given dataset and version",
		ArgsName: "<dataset> <version>",
		Runner:   v23cmd.RunnerFunc(runPreprocessedData),
	}
	cmd.Flags.StringVar(&outputFlag, "o", "", "Path to output dataset. Will write to tidydata cache by default.")
	return cmd
}

func main() {
	cmdline.HideGlobalFlagsExcept()
	cmdline.Main(cmdRoot())
}
