import certifi
from sys import argv


def main(ca_fp):
    with open(ca_fp, 'rb') as f:
        my_ca = f.read()

    ca_file = certifi.where()

    with open(ca_file, 'ab') as f:
        f.write(my_ca)

    print("%s updated" % ca_file)


if __name__ == '__main__':
    main(argv[1])
