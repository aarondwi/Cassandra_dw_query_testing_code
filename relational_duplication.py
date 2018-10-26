import psycopg2
import argparse
import gc

def duplicate(sql_query):
    global conn
    try:
        cur = conn.cursor()
        cur.execute(sql_query)
        conn.commit()
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)

parser = argparse.ArgumentParser(description='Duplicating Relational DW')

#0=> transaksi_fact, 1=> buku_fact, 2=> usulan_fact
parser.add_argument('ip', action="store",type=str)
parser.add_argument('tipe', action="store",type=int)
parser.add_argument('jumlah', action="store",type=int)
args = vars(parser.parse_args())

conn = psycopg2.connect(host=args['ip'],database="skripsi", user="skripsiuser", password="skripsipassword")

selected_query=[ \
	"INSERT INTO transaksi_fact (id_transaksi,buku_dim_key, tgl_pinjam_key, tgl_batas_key,tgl_kembali_key, jurusan_key, kode_anggota, denda, terbayar) SELECT * FROM transaksi_fact_dummy;",\
	"INSERT INTO buku_fact (buku_dim_key,tgl_datang_key,tgl_input_key) SELECT * FROM buku_fact_dummy;", \
	"INSERT INTO usulan_fact (id_usul, tgl_key, jurusan_key, kode_anggota, judul, penerbit, pengarang, jenis_usul, status) SELECT * FROM usulan_fact_dummy;" \
]

for i in range(0,args['jumlah']):
    duplicate(selected_query[args['tipe']])
    print("{} duplication done!".format(i+1))
    gc.collect() #preventing this code from using too much memory
conn.close()