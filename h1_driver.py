import json
import sys
import traceback

from utils import spark_config
from tasks.ingest import ingest
from tasks.preprocess import preprocess, transform as pre_process
from tasks.process import process, transform as final_process


def main():
    try:
        print("\nArguments passed:", end=" ")
        for i in range(1, len(sys.argv)):
            print(sys.argv[i], end=" ")

        task_name = sys.argv[1]
        print('*** Starting process ::'+task_name)

        # Dictionary to read all input and output path details.
        path_dict = json.load(open('config/paths.json'))

        """
        Create the SparkSession
        """
        spark = spark_config.get_spark_session('h1_test' + task_name)

        if 'ingest' == task_name:
            df = ingest(spark, path_dict)
            df = pre_process(df)
            df = final_process(df)
            df.show()
        elif 'preprocess' == task_name:
            preprocess(spark, path_dict)
        elif 'process' == task_name:
            process(spark, path_dict)
        else:
            SystemExit
    except:
        traceback.print_exc()
    finally:
        """
        Stop the SparkSession
        """
        spark.stop()


# Entry point for the App
if __name__ == '__main__':
    main()
