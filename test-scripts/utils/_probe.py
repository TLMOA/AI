import io, csv, requests
PIMA = "/home/yhz/iot/pima-dataset.csv"
def load(n=None):
    rows = []
    with open(PIMA, newline="") as f:
        r = csv.DictReader(f)
        for i, row in enumerate(r):
            if n is not None and i >= n: break
            rows.append(row)
    return rows
buf = io.StringIO()
rows = load(10); w = csv.DictWriter(buf, fieldnames=rows[0].keys()); w.writeheader(); w.writerows(rows)
data = buf.getvalue().encode()
s = requests.Session()
s.post("http://127.0.0.1:5174/api/v1/auth/login", json={"username":"admin","password":"admin"})
r = s.post("http://127.0.0.1:5174/api/v1/files/upload", files={"file":("t.csv",data,"text/csv")}, data={"hasTag":"false","description":"x"})
print("UPLOAD:", r.status_code, r.text[:500])
r2 = s.post("http://127.0.0.1:5174/api/v1/training/submit", json={"selectedFileIds":[],"trainingConfig":{}})
print("TRAIN_EMPTY:", r2.status_code, r2.text[:500])
r3 = s.post("http://127.0.0.1:5174/api/v1/training/submit", json={"selectedFileIds":["nope_xyz"],"trainingConfig":{}})
print("TRAIN_NOPE:", r3.status_code, r3.text[:500])
an = requests.Session()
ra = an.get("http://127.0.0.1:5174/api/v1/files?pageSize=5")
print("ANON_FILES:", ra.status_code, ra.text[:300])
