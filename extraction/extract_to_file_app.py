from .big_query_client import BigQueryClient
from .file_writer import FileWriter

def main():

    #Set up objects
    client = BigQueryClient()
    writer = FileWriter()

    #Send data from big query to a given file.
    # 500 is the limit of data points fetched.
    client.produce_json_data(writer, 500)


if __name__ == "__main__": main()
