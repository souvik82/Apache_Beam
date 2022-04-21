import apache_beam as beam
from pathlib import Path

def format_result(word, count):
    return '%s: %d' % (word, count)


def sortGroupedData(row):
    (keyNumber, sortData) = row
    sortData.sort(key=lambda x: x[1], reverse=True)
    return sortData


if __name__ == '__main__':

    ROOT_DIR = Path(__file__).parent.parent
    DATA_PATH = str(ROOT_DIR) + r"\test_data\word_list.txt"

    with beam.Pipeline() as p:
        counts = (
                p
                | 'Read Text File' >> beam.io.ReadFromText(DATA_PATH)
                | 'Flatten the Records' >> beam.FlatMap(lambda lines: lines.split(" "))
                | 'Map the Word' >> beam.Map(lambda records: (records, 1))
                | 'Group sum' >> beam.CombinePerKey(sum)
                | 'AddKey' >> beam.Map(lambda row: (1, row))
                | 'GroupByKey' >> beam.GroupByKey()
                | 'SortGroupedData' >> beam.Map(sortGroupedData)
                # | 'Group by key' >> beam.Flatten()
                # | 'Flatten the Data' >> beam.Map(lambda records: [i for i in records])
                # | 'Format the Records' >> beam.MapTuple(format_result)
                | 'Print the Records' >> beam.Map(print)
        )
