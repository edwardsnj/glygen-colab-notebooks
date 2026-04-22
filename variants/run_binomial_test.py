import os, sys
from optparse import OptionParser
import glob
import numpy as np
from scipy.stats import binomtest
import subprocess

__version__ = "1.0"
__status__ = "Dev"

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(SCRIPT_DIR, "data")


def load_map_dict(in_file):

    tmp_dict = {}
    with open(in_file, "r") as FR:
        f_list, idx = [], 0
        for line in FR:
            idx += 1
            row = line[1:-2].strip().split("\",\"")
            if idx == 1:
                f_list = row
            else:
                canon    = row[f_list.index("uniprot_canonical_ac")]
                site_pos = int(row[f_list.index("site_pos")])
                var_pos  = int(row[f_list.index("variant_pos")])
                rel_pos  = int(row[f_list.index("relative_pos")])
                cmb = "%s|%s|%s|%s" % (canon, site_pos, var_pos, rel_pos)
                tmp_dict[cmb] = True
    return tmp_dict


def run_binomial_test(in_file):

    print("Processing: %s" % in_file)
    map_dict = load_map_dict(in_file)
    count_dict = {}
    N = 0
    for cmb in map_dict:
        rel_pos = int(cmb.split("|")[-1])
        if rel_pos not in count_dict:
            count_dict[rel_pos] = 0
        count_dict[rel_pos] += 1
        N += 1

    p = 1.0 / float(len(count_dict.keys()))

    file_name = os.path.basename(in_file)
    out_file = os.path.join(DATA_DIR, "binomial_test.%s" % file_name)
    newrow = ["relative_pos", "p_value"]
    FW = open(out_file, "w")
    FW.write("\"%s\"\n" % ("\",\"".join(newrow)))
    for rel_pos in sorted(count_dict):
        k = count_dict[rel_pos]
        p_value = binomtest(k, N, p, alternative='two-sided').pvalue
        newrow = [str(rel_pos), str(p_value)]
        FW.write("\"%s\"\n" % ("\",\"".join(newrow)))
    FW.close()

    cmd = "chmod 775 " + out_file
    subprocess.getoutput(cmd)
    print("Written: %s" % out_file)


###############################
def main():

    usage = "\n%prog  [options]"
    parser = OptionParser(usage, version="%prog version___")
    parser.add_option("-i", "--infile",  action="store",      dest="infile",   help="single mapping file to process")
    parser.add_option("-a", "--all",     action="store_true", dest="run_all",  help="run binomial test on all mapping files in data/", default=False)

    (options, args) = parser.parse_args()

    if not options.run_all and not options.infile:
        parser.print_help()
        sys.exit(0)

    if options.run_all:
        file_list = glob.glob(os.path.join(DATA_DIR, "mapping.human.*.csv"))
        if not file_list:
            print("No mapping files found in %s" % DATA_DIR)
            sys.exit(0)
        print("Found %s mapping file(s) to process" % len(file_list))
    else:
        file_list = [options.infile]

    for in_file in file_list:
        run_binomial_test(in_file)

    return


if __name__ == '__main__':
    main()
