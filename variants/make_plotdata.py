import os, sys
from optparse import OptionParser
import glob
import json
import matplotlib.pyplot as plt
import numpy as np

__version__ = "1.0"
__status__ = "Dev"

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(SCRIPT_DIR, "data")
PLOTS_DIR = os.path.join(SCRIPT_DIR, "plots")


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


def load_binomial_results(in_file):
    p_dict = {}
    with open(in_file, "r") as FR:
        f_list, idx = [], 0
        for line in FR:
            idx += 1
            row = line.strip().strip('"').split('","')
            if idx == 1:
                f_list = row
            else:
                rel_pos = int(row[f_list.index("relative_pos")])
                p_value = float(row[f_list.index("p_value")])
                p_dict[rel_pos] = p_value
    return p_dict


def get_star(p):
    if p < 0.001:
        return '***'
    elif p < 0.01:
        return '**'
    elif p < 0.05:
        return '*'
    return ''


###############################
def main():

    usage = "\n%prog  [options]"
    parser = OptionParser(usage, version="%prog version___")
    parser.add_option("-g", "--glycotype", action="store", dest="glycotype", help="N-linked/O-linked")
    parser.add_option("-v", "--vartype",   action="store", dest="vartype",   help="all/somatic/germline")
    parser.add_option("-t", "--sitetype",  action="store", dest="sitetype",  help="predicted/experimental")

    (options, args) = parser.parse_args()
    for key in ([options.glycotype, options.vartype, options.sitetype]):
        if not (key):
            parser.print_help()
            sys.exit(0)

    glyco_type = options.glycotype
    v_type     = options.vartype
    site_type  = options.sitetype

    os.makedirs(PLOTS_DIR, exist_ok=True)

    gt = glyco_type.lower().replace("-", "_")

    ptrn, out_cat = "*", "all"
    if v_type == "somatic":
        ptrn, out_cat = "somatic_cancer", "somatic"
    elif v_type == "germline":
        ptrn, out_cat = "germline", "germline"

    file_list = glob.glob(os.path.join(DATA_DIR, "mapping.human.%s.%s_%s.csv" % (gt, ptrn, site_type)))
    print("\nFiles found for plotting:")
    for f in file_list:
        print("  %s" % f)

    out_doc = {}
    min_x, max_x = 10000, -1000
    for in_file in file_list:
        var_type = in_file.split(".")[-2]
        out_doc[var_type] = {"rel_pos": [], "count": [], "ratio": [], "fc": []}
        map_dict = load_map_dict(in_file)
        stat_dict = {}
        total = 0
        for cmb in map_dict:
            rel_pos = int(cmb.split("|")[-1])
            min_x = rel_pos if rel_pos < min_x else min_x
            max_x = rel_pos if rel_pos > max_x else max_x
            if rel_pos not in stat_dict:
                stat_dict[rel_pos] = 0
            stat_dict[rel_pos] += 1
            total += 1
        for rel_pos in sorted(stat_dict):
            count = stat_dict[rel_pos]
            ratio = round(float(count) / float(total), 4)
            out_doc[var_type]["rel_pos"].append(rel_pos)
            out_doc[var_type]["count"].append(count)
            out_doc[var_type]["ratio"].append(ratio)

    for var_type in out_doc:
        for rel_pos in out_doc[var_type]["rel_pos"]:
            idx = out_doc[var_type]["rel_pos"].index(rel_pos)
            own_ratio = out_doc[var_type]["ratio"][idx]
            others_sum = 0.0
            for i in range(0, len(out_doc[var_type]["ratio"])):
                if i == idx:
                    continue
                others_sum += out_doc[var_type]["ratio"][i]
            others_mean = round(others_sum / float(len(out_doc[var_type]["ratio"]) - 1), 4)
            fc = round(own_ratio / others_mean, 4)
            out_doc[var_type]["fc"].append(fc)

    out_file = os.path.join(PLOTS_DIR, "plotdata.human.%s.%s_%s.json" % (gt, out_cat, site_type))
    FW = open(out_file, "w")
    FW.write("%s\n" % (json.dumps(out_doc, indent=4)))
    FW.close()
    print("\nPlot data written: %s" % out_file)

    plt.figure(figsize=(10, 5))

    ax = plt.gca()
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)

    plt.rcParams.update({
        'font.size': 16,
        'axes.titlesize': 16,
        'axes.labelsize': 16,
        'xtick.labelsize': 18,
        'ytick.labelsize': 18,
        'legend.fontsize': 14
    })

    plt.rcParams['axes.linewidth'] = 2.5
    plt.rcParams['xtick.major.width'] = 2
    plt.rcParams['ytick.major.width'] = 2
    plt.rcParams['xtick.minor.width'] = 1.5
    plt.rcParams['ytick.minor.width'] = 1.5

    color_map = {
        "germline_predicted":          "blue",
        "germline_experimental":       "blue",
        "somatic_cancer_predicted":    "red",
        "somatic_cancer_experimental": "red",
    }

    for var_type in out_doc:
        x = out_doc[var_type]["rel_pos"]
        y = out_doc[var_type]["fc"]
        label = var_type.replace("_all_all_", "_").replace("_all_", "_")
        color = color_map.get(label, "black")
        plt.plot(x, y, label=label, linestyle='-', color=color, linewidth=2.5)

        binomial_file = os.path.join(DATA_DIR, "binomial_test.mapping.human.%s.%s.csv" % (gt, var_type))
        if os.path.exists(binomial_file):
            p_dict = load_binomial_results(binomial_file)
            for rel_pos, fc_val in zip(x, y):
                if rel_pos in p_dict:
                    star = get_star(p_dict[rel_pos])
                    if star:
                        y_min, y_max = plt.ylim()
                        y_range = y_max - y_min
                        offset = -(y_range * 0.02) if fc_val < 1.0 else (y_range * 0.01)
                        va = 'top' if fc_val < 1.0 else 'bottom'
                        plt.text(rel_pos, fc_val + offset, star,
                                 ha='center', va=va, fontsize=14, color=color)
        else:
            print("Warning: no binomial results file found for %s" % var_type)

    plt.title("%s Glycosylation - %s" % (glyco_type, site_type), fontsize=18, pad=15)
    plt.xlabel("Relative Amino Acid Position", fontsize=16)
    plt.ylabel("Fold Change Obs vs Exp", fontsize=16)
    plt.xticks(np.arange(-6, 7, 1), fontsize=14)
    plt.yticks(fontsize=14)
    plt.xlim(-6, 6)
    plt.legend(loc='lower left')

    png_file = os.path.join(PLOTS_DIR, "plot.human.%s.%s_%s.png" % (gt, out_cat, site_type))
    plt.savefig(png_file)
    print("Plot saved: %s" % png_file)

    return


if __name__ == '__main__':
    main()
