import os, sys
from optparse import OptionParser

__version__ = "1.0"
__status__ = "Dev"

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(SCRIPT_DIR, "data")


def load_site_dict(in_file, glyco_type):

    tmp_dict = {}
    with open(in_file, "r") as FR:
        f_list, idx = [], 0
        for line in FR:
            idx += 1
            row = line[1:-2].strip().split("\",\"")
            if idx == 1:
                f_list = row
            else:
                gtype = row[f_list.index("gtype")]
                if gtype.lower() != glyco_type.lower():
                    continue
                canon = row[f_list.index("uniprot_canonical_ac")]
                start_pos = int(row[f_list.index("start_pos")])
                site = ":".join(row[2:])
                if canon not in tmp_dict:
                    tmp_dict[canon] = {}
                if start_pos not in tmp_dict[canon]:
                    tmp_dict[canon][start_pos] = {}
                tmp_dict[canon][start_pos][site] = True
    return tmp_dict


def load_var_dict(in_file, v_type):
    tmp_dict = {}
    with open(in_file, "r") as FR:
        f_list, idx = [], 0
        for line in FR:
            idx += 1
            row = line[1:-2].strip().split("\",\"")
            if idx == 1:
                f_list = row
            else:
                canon = row[f_list.index("uniprot_canonical_ac")]
                start_pos = int(row[f_list.index("start_pos")])
                variant_type = row[f_list.index("variant_type")]
                if v_type.lower() != variant_type:
                    continue
                var = ":".join(row[2:])
                if canon not in tmp_dict:
                    tmp_dict[canon] = {}
                if start_pos not in tmp_dict[canon]:
                    tmp_dict[canon][start_pos] = {}
                tmp_dict[canon][start_pos][var] = True

    return tmp_dict


###############################
def main():

    usage = "\n%prog  [options]"
    parser = OptionParser(usage, version="%prog version___")
    parser.add_option("-g", "--glycotype", action="store", dest="glycotype", help="N-linked/O-linked")
    parser.add_option("-v", "--vartype",   action="store", dest="vartype",   help="somatic_cancer/germline")
    parser.add_option("-t", "--sitetype",  action="store", dest="sitetype",  help="predicted/experimental")

    (options, args) = parser.parse_args()
    for key in ([options.glycotype, options.vartype, options.sitetype]):
        if not (key):
            parser.print_help()
            sys.exit(0)

    glyco_type = options.glycotype
    v_type     = options.vartype

    in_file = os.path.join(DATA_DIR, "human_glycosites_%s.csv" % options.sitetype)
    site_dict = load_site_dict(in_file, glyco_type)

    in_file = os.path.join(DATA_DIR, "human_variants_missense.csv")
    var_dict = load_var_dict(in_file, v_type)

    gt = glyco_type.lower().replace("-", "_")

    out_file = os.path.join(DATA_DIR, "mapping.human.%s.%s_%s.csv" % (gt, v_type, options.sitetype))

    newrow = ["uniprot_canonical_ac", "site_pos", "variant_pos", "relative_pos", "site_info", "variant_info"]
    FW = open(out_file, "w")
    FW.write("\"%s\"\n" % ("\",\"".join(newrow)))
    for canon in site_dict:
        if canon not in var_dict:
            continue
        for s_pos in site_dict[canon]:
            for site in site_dict[canon][s_pos]:
                for v_pos in var_dict[canon]:
                    for var in var_dict[canon][v_pos]:
                        diff = s_pos - v_pos
                        if abs(diff) > 20:
                            continue
                        newrow = [canon, str(s_pos), str(v_pos), str(diff), site, var]
                        FW.write("\"%s\"\n" % ("\",\"".join(newrow)))
    FW.close()

    return


if __name__ == '__main__':
    main()