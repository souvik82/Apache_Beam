import apache_beam as beam

if __name__ == '__main__':
    with beam.Pipeline() as p:
        test = (p
                | 'Read Data' >> beam.io.ReadFromText(r"C:\Users\INE12377731\Data\actual.csv", skip_header_lines=1)
                | 'Split Data' >> beam.Map(lambda line: line.split(','))
                | 'Filter Data' >> beam.Filter(lambda records: records[6] == '1065')
                | 'Join Data' >> beam.Map(lambda records: ",". join(records))
                | 'Write to Local File' >> beam.io.WriteToText(r"C:\Users\INE12377731\Data\actual", ".csv")
                )
