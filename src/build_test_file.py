## building a workable testing file

def build_test_file(filename, lines=10000):

    with open(f"../data/{filename}") as big_file:
        name_list = filename.split(".")
        name_list[0] += (f"_{lines}")
        mini_filename = ".".join(name_list)

        w = open(mini_filename, "w+")

        count = lines
        for line in big_file:
            if count == 0:
                break
            w.write(line)
            count -= 1

        w.close()

def build_sample(filename, lines=10000):
    pass

if __name__ == "__main__":
    build_test_file("SharedResponsesSurvey.csv")

