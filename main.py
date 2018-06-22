import json
import os
import pandas as pd
from os import listdir
from os.path import isfile, join
from pprint import pprint

main_fields = ['twitter', 'facebook', 'en.wikipedia.org', 'wikidata', 'instagram', 'linkedin', 'pinterest', 'youtube',
               'google']
extra_fields = []
json_path = 'json'
csv_path = 'csv'
main_csv = 'main.csv'
extra_csv = 'extra.csv'
main_fields_list = []
extra_fields_list = []


def save_to_csv(output_csv, fields_list, columns):
    matrix = []
    for item in fields_list:
        row = []
        row.append(item['url'])
        for column in columns:
            if item['key'] == column:
                row.append(item['value'])
            else:
                row.append('')
        matrix.append(row)
    df = pd.DataFrame(matrix, columns=['url'] + columns)

    # Merge duplicate rows
    for column in columns:
        df[column] = df.groupby(['url'])[column].transform(lambda x: ' '.join(x))

    # Remove duplicate rows
    df = df.drop_duplicates(['url'], keep='first')

    df_obj = df.select_dtypes(['object'])
    df[df_obj.columns] = df_obj.apply(lambda x: x.str.strip())

    df = df.applymap(lambda x: " ".join(sorted(set(x.split()), key=x.split().index)))

    # df = df.applymap(lambda x: x.strip() if type(x) is str else x)
    df.to_csv(output_csv, sep='\t', encoding='utf-8', index=False)


def json_to_csv(file_name):
    with open(file_name) as f:
        data = json.load(f)
    for item in data:
        # get url
        if 'url' not in item or 'result' not in item:
            continue
        url = item['url']

        # get result
        result = json.loads(item['result'])
        if not result or 'links' not in result:
            continue

        # get link
        for link in result['links']:
            for key, value in link.iteritems():
                if key.lower() in main_fields:
                    main_fields_list.append({'url': url, 'key': key.lower(), 'value': value})
                else:
                    extra_fields_list.append({'url': url, 'key': key.lower(), 'value': value})


def main():
    file_list = [f for f in listdir(json_path) if isfile(join(json_path, f))]
    print('--------------- Scraping/Parsing Json file progress ---------------')
    for file_name in file_list:
        json_to_csv(os.path.join(json_path, file_name))
    print('--------------- Scraping/Parsing Json file completed ---------------')

    print('--------------- Saving to main.csv file ---------------')
    save_to_csv(os.path.join(csv_path, main_csv), main_fields_list, main_fields)
    print('--------------- main.csv file already saved ---------------')

    for item in extra_fields_list:
        if item['key'] not in extra_fields:
            extra_fields.append(item['key'])
    print('--------------- Saving to extra.csv file ---------------')
    save_to_csv(os.path.join(csv_path, extra_csv), extra_fields_list, extra_fields)
    print('--------------- extra.csv file already saved ---------------')


if __name__ == '__main__':
    main()

