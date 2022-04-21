import apache_beam as beam
import csv


class splitRecords(beam.DoFn):
    def process(self, element, side_list):
        reader = csv.reader([element], delimiter=',', quotechar='"')
        records = next(reader)
        if int(records[0].strip()) not in side_list:
            return [(int(records[0].strip()), records[1].strip(), records[6].strip(),
                     float(records[8].strip().replace(',', '')))]


class myPTransformation(beam.PTransform):

    def expand(self, input_col):
        result = (
                input_col
                | 'Data Cleanup' >> beam.Map(lambda data: (data[0], data[1].replace(' ', '_'), data[2], data[3]))
        )
        return result