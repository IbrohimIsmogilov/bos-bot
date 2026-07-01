import sys, io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
from course_data import COURSES
r = COURSES["bos"]["roadmap"]
print(f"OK: {r['title']}")
print(f"Topics: {len(r['topics'])}")
for t in r["topics"]:
    mm, ss = divmod(t["startSeconds"], 60)
    hh, mm2 = divmod(mm, 60)
    print(f"  [{hh:02d}:{mm2:02d}:{ss:02d}] {t['title']}")
