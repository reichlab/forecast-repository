;; -*- mode: Outline;-*-

This directory contains a very small version of the [COVID-19 Forecasts](https://zoltardata.com/project/44) Zoltar project that's used for testing visualization. It contains the following data:


* timezeros: ["2022-01-03", "2022-01-10", "2022-01-17", "2022-01-24", "2022-01-31"]


* units: ["US", "48"]


* targets: ["1 wk ahead inc death", "2 wk ahead inc death"]


* forecasts
JSON files corresponding to each forecast were downloaded from each forecast's page and then edited to include only the relevant predictions using the small Python utility `filter_json_files()`. Files were saved to forecasts-json-small/ .


** filter_json_files()
def filter_json_files():
    # process full forecast json files, keeping only those predictions that are for our limited units and targets
    json_dir_in = Path('/Users/cornell/Downloads/covid-viz-test-project/forecasts-json-orig')
    json_dir_out = Path('/Users/cornell/Downloads/covid-viz-test-project/forecasts-json-small')
    for json_in_file in json_dir_in.glob('*.json'):
        with open(json_in_file) as fp:
            json_io_dict_in = json.load(fp)
            print(f"{json_in_file!r}, {len(json_io_dict_in['predictions'])}")

        # filter predictions
        units = ["US", "48"]  # abbreviations. 48=TX
        targets = ["1 wk ahead inc death", "2 wk ahead inc death"]  # name
        predictions = [prediction for prediction in json_io_dict_in['predictions']
                       if prediction['unit'] in units and prediction['target'] in targets]

        print(f"{len(predictions)}")
        with open(json_dir_out / json_in_file.name, 'w') as fp:
            json.dump({"meta": {}, "predictions": predictions}, fp, indent=4)


* truth
Obtained from zoltar-truth.csv git revisions for the relevant timezeros (plus a few before them) via a command like:
  git checkout <commit_hash> -- data-truth/zoltar-truth.csv

Files were saved to truths-orig/ and then edited as in forecasts using `filter_csv_files()`.

List of revisions via this command:
  git log --pretty="%h|%cd" -- data-truth/zoltar-truth.csv

    1d85ba3e9|Sun Jan 30 17:28:25 2022 +0000
    b7987099e|Sun Jan 23 16:58:11 2022 +0000
    bb98fe32c|Sun Jan 16 17:16:26 2022 +0000
    fc252a233|Sun Jan 9 16:57:07 2022 +0000
    c8dbd265b|Sun Jan 2 17:01:26 2022 +0000
    9808d47d0|Sun Dec 26 17:12:08 2021 +0000
    0a507f66c|Sun Dec 19 16:53:13 2021 +0000

Each revision extracted via something like this command:
  git checkout 1d85ba3e9  -- data-truth/zoltar-truth.csv
  cp data-truth/zoltar-truth.csv /tmp
  # manually mv /tmp/zoltar-truth.csv to the name: "<hash>-zoltar-truth.csv", e.g., "1d85ba3e9-zoltar-truth.csv"

When done:
  git reset --hard


** filter_csv_files()
def filter_csv_files():
    first_timezero = dateutil.parser.parse('2021-12-12')
    units = ["US", "48"]  # abbreviations. 48=TX
    targets = ["1 wk ahead inc death", "2 wk ahead inc death"]  # name

    # process full truth csv files, keeping only those rows that are for our limited units and targets
    csv_in_dir = Path('/Users/cornell/Downloads/covid-viz-test-project/truths-orig')
    csv_dir_out = Path('/Users/cornell/Downloads/covid-viz-test-project/truths-small')
    for csv_in_file in csv_in_dir.glob('*.csv'):
        # timezero,unit,target,value
        # 2020-03-15,US,1 wk ahead cum death,482
        print(csv_in_file)
        with open(csv_in_file, 'r') as csv_in_fp, \
                open(csv_dir_out / csv_in_file.name, 'w') as csv_out_fp:
            csv_reader = csv.reader(csv_in_fp)
            csv_writer = csv.writer(csv_out_fp)
            for idx, (timezero, unit, target, value) in enumerate(csv_reader):
                if (idx == 0) or ((dateutil.parser.parse(timezero) >= first_timezero)
                                  and (unit in units)
                                  and (target in targets)):
                    csv_writer.writerow([timezero, unit, target, value])

