import apache_beam as beam
from pathlib import Path

side_list = [2013, 2014, 2015, 2016, 2017, 2018]

if __name__ == '__main__':
    import sys

    ROOT_DIR = Path(__file__).parent.parent
    DATA_PATH = str(ROOT_DIR) + r"\test_data\survey.csv"
    print(ROOT_DIR)
    sys.path.append(str(ROOT_DIR) + r'\utils')
    import utilities

    with beam.Pipeline() as p:
        category_data = (
                p
                | 'Read Data From Source' >> beam.io.ReadFromText(DATA_PATH,
                                                                  skip_header_lines=1)
                | 'Split Records and discard Invalid Records with Side List' >> beam.ParDo(utilities.splitRecords(),
                                                                                           side_list)
                | 'My pTransformation' >> utilities.myPTransformation()
                # | 'Print Records' >> beam.Map(lambda line: print(line))
        )

        level_1 = (
                category_data
                | 'Filter Level 1' >> beam.Filter(lambda records: records[1].lower() == "level_1")
                | 'Print Level 1 Records' >> beam.Map(lambda line: print(line))
        )

        level_4 = (
                category_data
                | 'Filter Level 2' >> beam.Filter(lambda records: records[1].lower() == "level_4")
                | 'Print Level 2 Records' >> beam.Map(lambda line: print(line))
        )

        level_3 = (
                category_data
                | 'Filter Level 3' >> beam.Filter(lambda records: records[1].lower() == "level_3")
                | 'Print Level 3 Records' >> beam.Map(lambda line: print(line))
        )

    p.run()
