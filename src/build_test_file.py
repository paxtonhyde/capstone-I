## for building a reasonably sized file for testing scripts
import argparse

def build_test_file(filename, lines=10000):

    with open(f"../data/{filename}") as big_file:
        ## write the filename: ..._lines.csv
        name_list = filename.split(".")
        name_list[0] += (f"_{lines}")
        mini_filename = ".".join(name_list)

        w = open(f"../data/{mini_filename}", "w+")

        count = lines
        for line in big_file:
            if count == 0:
                break
            w.write(line)
            count -= 1

        w.close()

if __name__ == "__main__":
    ## argument parsing
    parser = argparse.ArgumentParser()
    parser.add_argument("-d", "--desc", help="""This program takes a file a returns a copy 
                        with the first -n lines (default 10000). \n Usage : build_test_file.py [--file] [--lines]""")
    parser.add_argument('-f', '--file', required=True,\
                         help='File to pare down.')
    parser.add_argument('-n', '--lines', default=10000,\
                         help='Number of lines to write to new file.')
    args = vars(parser.parse_args())

    build_test_file(args['file'], args['lines'])

