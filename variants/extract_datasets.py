import os, sys
from optparse import OptionParser
import glob

__version__ = "1.0"
__status__ = "Dev"

SCRIPT_DIR   = os.path.dirname(os.path.abspath(__file__))
REVIEWED_DIR = os.path.join(SCRIPT_DIR, "reviewed")
SPECIES      = "human"


def extract_glyco_sites():

    file_list = glob.glob(os.path.join(REVIEWED_DIR, "human_proteoform_glycosylation_sites_*.csv"))

    out_file_exp  = os.path.join(data_dir, "human_glycosites_experimental.csv")
    out_file_pred = os.path.join(data_dir, "human_glycosites_predicted.csv")

    header = ["uniprot_canonical_ac", "start_pos", "start_aa", "gtype"]

    tmp_dict_exp  = {}
    tmp_dict_pred = {}

    uniprotkb_file = os.path.join(REVIEWED_DIR, "human_proteoform_glycosylation_sites_uniprotkb.csv")

    # ── Non-UniProtKB files ───────────────────────────────────────────────────
    print("\n=== FILE ROUTING ===")
    for in_file in file_list:
        if in_file.find(".stat.csv") != -1:
            print("SKIPPING (stat): %s" % in_file)
            continue
        if in_file == uniprotkb_file:
            print("SKIPPING (uniprotkb, handled separately): %s" % in_file)
            continue

        is_predicted = in_file.find("_predicted_") != -1

        if is_predicted:
            print("PREDICTED: %s" % in_file)
        else:
            print("EXPERIMENTAL: %s" % in_file)

        with open(in_file, "r") as FR:
            f_list, idx = [], 0
            for line in FR:
                idx += 1
                row = line[1:-2].split("\",\"")
                if idx == 1:
                    f_list = row
                    continue
                canon     = row[f_list.index("uniprotkb_canonical_ac")]
                start_pos = row[f_list.index("start_pos")]
                start_aa  = row[f_list.index("start_aa")]
                g_type    = row[f_list.index("glycosylation_type")]
                if canon == "" or start_pos == "":
                    continue
                # use protein + position as the key for cross-dataset comparison
                pos_key = "%s:%s:%s" % (canon, start_pos, g_type)
                site    = "%s:%s:%s:%s" % (canon, start_pos, start_aa, g_type)
                if is_predicted:
                    if site not in tmp_dict_pred:
                        tmp_dict_pred[site] = {
                            "row": [canon, str(start_pos), start_aa, g_type],
                            "pos_key": pos_key
                        }
                else:
                    if site not in tmp_dict_exp:
                        tmp_dict_exp[site] = {
                            "row": [canon, str(start_pos), start_aa, g_type],
                            "pos_key": pos_key
                        }

    # ── UniProtKB file: first pass to find experimentally supported sites ─────
    print("\n=== UNIPROTKB FILE ===")
    uniprotkb_exp_sites = set()
    if os.path.exists(uniprotkb_file):
        print("FOUND: %s" % uniprotkb_file)
        with open(uniprotkb_file, "r") as FR:
            f_list, idx = [], 0
            for line in FR:
                idx += 1
                row = line[1:-2].split("\",\"")
                if idx == 1:
                    f_list = row
                    continue
                canon     = row[f_list.index("uniprotkb_canonical_ac")]
                start_pos = row[f_list.index("start_pos")]
                start_aa  = row[f_list.index("start_aa")]
                g_type    = row[f_list.index("glycosylation_type")]
                xref_key  = row[f_list.index("xref_key")]
                if canon == "" or start_pos == "":
                    continue
                if xref_key.find("_xref_pubmed") != -1 or xref_key.find("_xref_doi") != -1:
                    site = "%s:%s:%s:%s" % (canon, start_pos, start_aa, g_type)
                    uniprotkb_exp_sites.add(site)
        print("UniProtKB experimental sites found: %s" % len(uniprotkb_exp_sites))

        # ── UniProtKB file: second pass to write to exp or pred ──────────────
        with open(uniprotkb_file, "r") as FR:
            f_list, idx = [], 0
            for line in FR:
                idx += 1
                row = line[1:-2].split("\",\"")
                if idx == 1:
                    f_list = row
                    continue
                canon     = row[f_list.index("uniprotkb_canonical_ac")]
                start_pos = row[f_list.index("start_pos")]
                start_aa  = row[f_list.index("start_aa")]
                g_type    = row[f_list.index("glycosylation_type")]
                if canon == "" or start_pos == "":
                    continue
                pos_key = "%s:%s:%s" % (canon, start_pos, g_type)
                site    = "%s:%s:%s:%s" % (canon, start_pos, start_aa, g_type)
                if site in uniprotkb_exp_sites:
                    if site not in tmp_dict_exp:
                        tmp_dict_exp[site] = {
                            "row": [canon, str(start_pos), start_aa, g_type],
                            "pos_key": pos_key
                        }
                else:
                    if site not in tmp_dict_pred:
                        tmp_dict_pred[site] = {
                            "row": [canon, str(start_pos), start_aa, g_type],
                            "pos_key": pos_key
                        }
    else:
        print("NOT FOUND: %s" % uniprotkb_file)

    # ── Remove predicted sites that appear as experimental ───────────────────
    print("\n=== CROSS-DATASET FILTERING ===")

    # build a set of all experimental pos_keys (protein + position + gtype)
    exp_pos_keys = set(tmp_dict_exp[site]["pos_key"] for site in tmp_dict_exp)

    # filter predicted sites — remove any whose pos_key matches an experimental site
    filtered_pred = {}
    removed_count = 0
    for site in tmp_dict_pred:
        pos_key = tmp_dict_pred[site]["pos_key"]
        if pos_key in exp_pos_keys:
            removed_count += 1
        else:
            filtered_pred[site] = tmp_dict_pred[site]

    print("Predicted sites removed (found in experimental): %d" % removed_count)
    print("Predicted sites remaining: %d" % len(filtered_pred))

    # ── Write experimental sites ──────────────────────────────────────────────
    FW_exp = open(out_file_exp, "w")
    FW_exp.write("\"%s\"\n" % ("\",\"".join(header)))
    for site in tmp_dict_exp:
        FW_exp.write("\"%s\"\n" % ("\",\"".join(tmp_dict_exp[site]["row"])))
    FW_exp.close()

    # ── Write filtered predicted sites ───────────────────────────────────────
    FW_pred = open(out_file_pred, "w")
    FW_pred.write("\"%s\"\n" % ("\",\"".join(header)))
    for site in filtered_pred:
        FW_pred.write("\"%s\"\n" % ("\",\"".join(filtered_pred[site]["row"])))
    FW_pred.close()

    print("\n=== SUMMARY ===")
    print("Total experimental sites written: %d" % len(tmp_dict_exp))
    print("Total predicted sites written:    %d" % len(filtered_pred))

    return


def extract_variants():

    file_list = [
        os.path.join(REVIEWED_DIR, "human_protein_mutation_germline_all.csv"),
        os.path.join(REVIEWED_DIR, "human_protein_mutation_cancer_all.csv")
    ]

    out_file = os.path.join(data_dir, "human_variants_missense.csv")
    FW = open(out_file, "w")
    newrow = ["uniprot_canonical_ac", "start_pos", "ref_aa", "alt_aa", "variant_type", "disease_status"]
    FW.write("\"%s\"\n" % ("\",\"".join(newrow)))
    tmp_dict = {}
    for in_file in file_list:
        is_cancer    = in_file.find("cancer") != -1
        variant_type = "somatic_cancer" if is_cancer else "germline"
        with open(in_file, "r") as FR:
            f_list, idx = [], 0
            for line in FR:
                idx += 1
                row = line[1:-2].split("\",\"")
                if idx == 1:
                    f_list = row
                    continue
                canon = row[f_list.index("uniprotkb_canonical_ac")]
                if is_cancer:
                    start_pos = int(row[f_list.index("aa_pos")])
                    end_pos   = int(row[f_list.index("aa_pos")])
                else:
                    start_pos = int(row[f_list.index("begin_aa_pos")])
                    end_pos   = int(row[f_list.index("end_aa_pos")])
                ref_aa, alt_aa = row[f_list.index("ref_aa")], row[f_list.index("alt_aa")]
                if is_cancer:
                    disease_description = row[f_list.index("do_name")]
                    d_status = "yes" if disease_description != "" else "no"
                else:
                    do_id, mim_id = row[f_list.index("do_id")], row[f_list.index("mim_id")]
                    d_status = "no" if do_id == "" and mim_id == "" else "yes"
                site = "%s:%s:%s>%s:%s" % (canon, start_pos, ref_aa, alt_aa, d_status)
                if start_pos > 1 and start_pos == end_pos and ref_aa != alt_aa:
                    if site not in tmp_dict:
                        newrow = [canon, str(start_pos), ref_aa, alt_aa, variant_type, d_status]
                        FW.write("\"%s\"\n" % ("\",\"".join(newrow)))
                    tmp_dict[site] = True
    FW.close()
    print("\n=== SUMMARY ===")
    print("Total missense variants written: %d" % len(tmp_dict))
    return


###############################
def main():

    usage = "\n%prog  [options]"
    parser = OptionParser(usage, version="%prog version___")
    parser.add_option("-d", "--dataset", action="store", dest="dataset", help="glycosites/variants")
    (options, args) = parser.parse_args()
    if not options.dataset:
        parser.print_help()
        sys.exit(0)

    global log_file
    global data_dir

    data_dir = os.path.join(SCRIPT_DIR, "data")
    os.makedirs(data_dir, exist_ok=True)

    if options.dataset == "glycosites":
        extract_glyco_sites()
    elif options.dataset == "variants":
        extract_variants()

    return


if __name__ == '__main__':
    main()