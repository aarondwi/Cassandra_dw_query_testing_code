from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import argparse
import datetime
import time
import uuid
import gc

#prepare all the query template
selected_get_query=[ \
	"SELECT * FROM cf_transaksi_standard_dummy",\
	"SELECT * FROM cf_buku_dummy",\
	"SELECT * FROM cf_transaksi_jurusan_kategori_dummy",\
	"SELECT * FROM cf_usulan_dummy",\
    "SELECT * FROM xyz"
]
#the insert's order should follow the name's order as listed in describe
selected_insert_query=[ \
	"INSERT INTO cf_transaksi_standard (tahun_ajaran, bulan, tanggal_pinjam, tanggal_batas, tanggal_kembali, unique_id, denda, fakultas, id_transaksi, kode_anggota, nama_jurusan, semester, terbayar) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?) ",\
	"INSERT INTO cf_buku (tahun, bulan, tanggal_datang, tanggal_input, unique_id, jenis_terbitan, judul, kelompok_kategori, kode_buku, kode_judul, nama_koleksi, penerbit, status_lama, status_sekarang, tanggal_ganti_status) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) ",\
	"INSERT INTO cf_transaksi_jurusan_kategori (nama_jurusan, tahun_ajaran, tgl_pinjam, kelompok_kategori, nama_koleksi, unique_id, denda, fakultas, id_transaksi, judul, kode_anggota, kode_buku, kode_judul, semester, status_lama, status_sekarang, tanggal_ganti_status, terbayar, tgl_batas, tgl_kembali) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) ",\
	"INSERT INTO cf_usulan (tahun, bulan, tgl_usulan, unique_id, fakultas,id_usulan, jenis_usul, judul, kode_anggota, nama_jurusan, penerbit, pengarang, status) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?) ",\
    "INSERT INTO xyz (a,b) values (?,?) "
]
selected_pos_unique_id=[5,4,5,3,1]

#parsing all arguments
parser = argparse.ArgumentParser(description='Duplicating Cassandra DW')
# 0=> cf_transaksi_standard, 1=> cf_buku, 
# 2=> cf_transaksi_jurusan_kategori, 3=> cf_usulan
# 4=> xyz (testing)
parser.add_argument('ip', action="store",type=str)
parser.add_argument('tipe', action="store",type=int)
parser.add_argument('jumlah', action="store",type=int)
args = vars(parser.parse_args())

#connecting to cassandra
auth_provider = PlainTextAuthProvider(username='cassandra', password='cassandra')
cluster=Cluster([args['ip']],auth_provider=auth_provider)
#cluster = Cluster(['192.168.38.23', '192.168.38.21','192.168.38.14'],auth_provider=auth_provider)
session = cluster.connect('skripsi')

results_orig=[]
rows = session.execute(selected_get_query[args['tipe']])
for row in rows:
    results_orig.append(list(row))

BATCH_SIZE=100
BATCH_SIZE_REST=len(results_orig)%BATCH_SIZE

def make_batch_statement(n,m):
    BATCH_STMT = 'BEGIN UNLOGGED BATCH '

    for i in range(n):
        BATCH_STMT += selected_insert_query[m]

    BATCH_STMT += 'APPLY BATCH;'
    return BATCH_STMT

len_of_params = BATCH_SIZE * len(results_orig[0])
l=len(results_orig)

waiting_time=10

for i in range(0,args['jumlah']):
    count=0
    prep_batch = session.prepare(make_batch_statement(BATCH_SIZE,args['tipe']))

    #copying data, and creating new unique_ids
    results=results_orig[:]
    for idx in range(0,l):
        results[idx][selected_pos_unique_id[args['tipe']]]=uuid.uuid4()

    #iterating through each data
    while count<len(results):
        # flattened all the data
        # because our statement are flattened
        flattened_data = [x for y in results[count:count+100] for x in y]

        try:
            # we check size here
            # because different prep_batch and flattened size results an error
            if len(flattened_data)==len_of_params:
                session.execute(prep_batch,flattened_data)
                count+=BATCH_SIZE
            else:
                prep_batch_the_rest= session.prepare(make_batch_statement(BATCH_SIZE_REST,args['tipe']))
                session.execute(prep_batch_the_rest,flattened_data)
                count+=BATCH_SIZE_REST
        except:
            #the most common error is write timeout, not the nodes failing
            #a trick here is sleep without increasing the count
            #so it will flattened the same data again
            #this trick will result in no duplicate
            #because we do NOT change the uuid 
            #means it will override existing data
            print("Sleep for {}s".format(waiting_time))
            time.sleep(waiting_time)

    gc.collect() #preventing this code from using too much memory
    print("{} duplication done!".format(i+1))