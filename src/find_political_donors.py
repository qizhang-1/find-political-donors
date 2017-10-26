import sys
import ReadFile as rf


def main():
    input_filename = sys.argv[1]
    output_filename_id_zip = sys.argv[2]
    output_filename_id_date = sys.argv[3]

    io = rf.InputOutput(input_filename, output_filename_id_zip, output_filename_id_date, '|')
    io.read_process_write()


if __name__ == "__main__":
    main()