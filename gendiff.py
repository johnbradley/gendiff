from __future__ import print_function

import ftplib
import cStringIO
import json
import sys
import csv
import os
from sqlalchemy import Column, String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
Base = declarative_base()
from sqlalchemy import create_engine

class GenDiff(object):
    config_file = "gendiff.json"

    def __init__(self, dir_name):
        self.dir_name = dir_name
        self._is_setup = False

    def setup(self):
        self._setup_dir()
        self._setup_config()
        self._is_setup = True

    def run_server(self):
        if not self._is_setup:
            self.setup()
        print(self.config.genome_ftp_url)
        genome_store = GenomeStore(self.dir_name, self.config)
        genome_store.setup_summary_file()

    def _setup_dir(self):
        if not os.path.isdir(self.dir_name):
            print("Creating directory", self.dir_name)
            os.makedirs(self.dir_name)

    def config_path(self):
        return os.path.join(self.dir_name, self.config_file)

    def _setup_config(self):
        config_path = self.config_path()
        config = Config()
        if not os.path.exists(config_path):
            print("Creating config file", config_path)
            config.write_to_path(config_path)
        config.load_from_path(config_path)
        self.config = config

class Config(object):
    def __init__(self):
        self.genome_ftp_url = "ftp.ncbi.nlm.nih.gov"
        self.refseq_dir = "genomes/refseq/"
        self.summary_file = "assembly_summary_refseq.txt"

    def write_to_path(self, path):
        with open(path, 'w') as outfile:
            json.dump(self.__dict__, outfile)

    def load_from_path(self, path):
        with open(path, 'r') as infile:
            json_dict = json.load(infile)
            self.set_from_dict(json_dict)

    def set_from_dict(self, json_dict):
        self.genome_ftp_url = self.get_val(json_dict,
                'genome_ftp_url', self.genome_ftp_url)
        self.refseq_dir = self.get_val(json_dict,
                'refseq_dir', self.refseq_dir)
        self.summary_file = self.get_val(json_dict,
                'summary_file', self.summary_file)

    def get_val(self, json_dict, name, def_val):
        if name in json_dict:
            value = json_dict[name]
            if value:
                return value
        return def_val

class GenomeStore(object):
    def __init__(self, data_dir, config):
        self.data_dir = data_dir
        self.config = config

    def summary_file_path(self):
        return os.path.join(self.data_dir, self.config.summary_file)

    def setup_summary_file(self):
        if not os.path.exists(self.summary_file_path()):
            self.download_summary_file()
        self.setup_summary_database()

    def download_summary_file(self):
        print("Downloading summary file.")
        ftp = ftplib.FTP(self.config.genome_ftp_url)
        ftp.login()
        ftp.cwd(self.config.refseq_dir)
        filename = self.config.summary_file
        path = self.summary_file_path()
        ftp.retrbinary("RETR " + filename ,open(path, 'wb').write)
        ftp.quit()

    def setup_summary_database(self):
        url = os.path.join('sqlite:///', self.data_dir, 'gendiff.sqlite')
        engine = create_engine(url, echo=True)

        print("create tables")
        Base.metadata.create_all(engine)
        session = sessionmaker(bind=engine)()
        with open(self.summary_file_path(), 'rb') as csvfile:
            pos = csvfile.tell()
            firstline = csvfile.readline()
            csvfile.seek(pos)
            if firstline.startswith("# "):
                csvfile.read(2)
            csvreader = csv.DictReader(csvfile, delimiter='\t', quotechar='|')
            row_names = [
                "assembly_accession",
                "bioproject",
                "biosample",
                "wgs_master",
                "refseq_category",
                "taxid",
                "species_taxid",
                "organism_name",
                "infraspecific_name",
                "isolate",
                "version_status",
                "assembly_level",
                "release_type",
                "genome_rep",
                "seq_rel_date",
                "asm_name",
                "submitter",
                "gbrs_paired_asm",
                "paired_asm_comp",
                "ftp_path"
            ]
            for row in csvreader:
                genome = Genome()
                for name in row_names:
                    val = unicode(row[name], "utf-8")
                    setattr(genome, name, val)
                session.add(genome)
                #print(row['assembly_accession'])
                """row = [unicode(s, "utf-8") for s in row]
                if row[0].startswith("#"):
                    row[0] = row[0].replace("# ","")
                    for idx,name in enumerate(row):
                        row_names[name] = idx
                    import pdb; pdb.set_trace()
                else:
                    genome = Genome(
                        assembly_accession",
                        bioproject",
                        biosample",
                        wgs_master",
                        refseq_category",
                        taxid",
                        species_taxid",
                        organism_name",
                        infraspecific_name",
                        isolate",
                        version_status",
                        assembly_level",
                        release_type",
                        genome_rep",
                        seq_rel_date",
                        asm_name",
                        submitter",
                        gbrs_paired_asm",
                        paired_asm_comp",
                        ftp_path = row[19])
                    session.add(genome)
                    """
        session.commit()
''
class SummaryDB(object):
    pass

#record = SeqIO.parse('GCF_000001765.3_Dpse_3.0_genomic.gbff', "genbank")
#r = record.next()
#[f for f in r.features if f.type == 'source']
#>>> r.features[0].type
#'source'
#r.features[0].qualifiers['chromosome']


class Genome(Base):
    __tablename__ = 'genome'
    assembly_accession = Column(String, primary_key=True)
    bioproject = Column(String)
    biosample = Column(String)
    wgs_master = Column(String)
    refseq_category = Column(String)
    taxid = Column(String)
    species_taxid = Column(String)
    organism_name = Column(String)
    infraspecific_name = Column(String)
    isolate = Column(String)
    version_status = Column(String)
    assembly_level = Column(String)
    release_type = Column(String)
    genome_rep = Column(String)
    seq_rel_date = Column(String)
    asm_name = Column(String)
    submitter = Column(String)
    gbrs_paired_asm = Column(String)
    paired_asm_comp = Column(String)
    ftp_path = Column(String)
    #assembly_accession	bioproject	biosample	wgs_master	refseq_category	taxid
    # species_taxid	organism_name	infraspecific_name	isolate	version_status
    # assembly_level	release_type	genome_rep	seq_rel_date	asm_name
    # submitter	gbrs_paired_asm	paired_asm_comp	ftp_path
    #GCF_000001215.4	PRJNA164	SAMN02803731		reference genome	7227
    # 7227	Drosophila melanogaster			latest	Chromosome	Major	Full
    # 2014/08/01	Release 6 plus ISO1 MT	The FlyBase Consortium/Berkeley
    # Drosophila Genome Project/Celera Genomics	GCA_000001215.4	identical
    #ftp://ftp.ncbi.nlm.nih.gov/genomes/all/GCF_000001215.4_Release_6_plus_ISO1_MT


def main(args = sys.argv):
    if len(args) != 2:
        print_usage()
    else:
        gendiff = GenDiff(args[1])
        gendiff.run_server()

def print_usage():
    print("python gendiff.py <data_dir>")

#data_dir = sys.argv[1]
#config_file = 'gendiff.json'
#print(data_dir)
if __name__ == "__main__":
    main()

#with open('data.json') as data_file:
#    data = json.load(data_file)

"""
def filter_non_genomes(dirlist):
    return [x for x in dirlist if not x.endswith(".txt")]

genome_ftp_url = "ftp.ncbi.nlm.nih.gov"
refseq_dir = "genomes/refseq/"
summary_file = "assembly_summary_refseq.txt"

sio = cStringIO.StringIO()
def handle_binary(more_data):
    sio.write(more_data)

ftp = FTP(genome_ftp_url)
ftp.login()
ftp.cwd('/genomes/refseq/')
resp = ftp.retrbinary("RETR " + summary_file, callback=handle_binary)
sio.seek(0) # Go back to the start
print sio.getvalue()
"""
#name = "class", value = "input-content label-is-floating is-invalid style-scope paper-input-container"
"""
root_dirs = [
    "archaea",
    "bacteria",
    "fungi",
    "invertebrate",
    "plant",
    "protozoa",
    "vertebrate_mammalian",
    "vertebrate_other",
    "viral"]

ftp = FTP(genome_ftp_url)
ftp.login()
ftp.cwd('/genomes/refseq/')
val = ftp.nlst()
for dirname in filter_non_genomes(val):
    print dirname
    print ftp.nlst(dirname)
    """
