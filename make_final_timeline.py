"""Draw the first 1 event/s trial per mode from recorded attempt and trial CSVs.
Uses Times New Roman at 8 pt; unconfirmed writes are not labeled as rejected writes.
"""
import csv
from pathlib import Path
from reportlab.pdfgen import canvas
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont
import shutil
import subprocess

ROOT = Path(__file__).resolve().parent
rows = list(csv.DictReader((ROOT/'results/e3_trials.csv').open()))
attempts = list(csv.DictReader((ROOT/'results/e3_attempts.csv').open()))
# Times New Roman is not redistributed; put the .ttf in fonts/ to match the paper, otherwise use
# reportlab's built-in Times-Roman.
TNR_FILE = ROOT / 'fonts' / 'Times New Roman.ttf'
if TNR_FILE.exists():
    pdfmetrics.registerFont(TTFont('TNR', str(TNR_FILE)))
    FONT = 'TNR'
else:
    FONT = 'Times-Roman'
path=ROOT/'results/fig_failover_timeline_final.pdf'
c=canvas.Canvas(str(path),pagesize=(252,158)); c.setFont(FONT,8)
x0=82; scale=9.3
x=lambda t:x0+float(t)*scale
blue=(.16,.47,.84); orange=(.92,.41,.20)
c.setFillColorRGB(.92,.92,.92);c.rect(x(0),35,x(8)-x(0),91,fill=1,stroke=0)
for xpos,y,color,label in [(26,146,orange,'Confirmed'),(128,146,blue,'Unconfirmed')]:
    c.setStrokeColorRGB(*color);c.setLineWidth(4);c.line(xpos,y,xpos+12,y)
    c.setFillColorRGB(0,0,0);c.drawString(xpos+16,y-3,label)
c.setStrokeColorRGB(0,0,0);c.setLineWidth(.8)
c.line(26,131,32,137);c.line(26,137,32,131);c.drawString(42,131,'Kill request')
c.line(134,130,134,138);c.drawString(144,131,'Writes resume')
for y,mode,label in [(116,'mongo-w1','MongoDB w:1'),(94,'mongo-majority','MongoDB majority'),(72,'redis-async','Redis async'),(50,'redis-wait','Redis WAIT')]:
    t=next(r for r in rows if r['mode']==mode and float(r['rate'])==1 and r['trial']=='1')
    aa=[r for r in attempts if r['mode']==mode and float(r['rate'])==1]; stamp=aa[0]['trial_start'];aa=[r for r in aa if r['trial_start']==stamp]
    c.setFillColorRGB(.15,.15,.15);c.drawRightString(x0-5,y-3,label)
    for r in aa:
        c.setStrokeColorRGB(*(orange if r['acked']=='1' else blue));c.setLineWidth(4)
        c.line(x(r['start_s']),y,x(max(float(r['end_s']),float(r['start_s'])+.08)),y)
    kill=float(t['kill_s']);resume=kill+float(t['unavailable_s'])
    c.setStrokeColorRGB(.3,.3,.3);c.setLineWidth(.8);c.setDash(1,2);c.line(x(kill),y,x(resume),y);c.setDash()
    c.setStrokeColorRGB(0,0,0);c.setLineWidth(1.2)
    c.line(x(kill)-2.5,y-2.5,x(kill)+2.5,y+2.5);c.line(x(kill)-2.5,y+2.5,x(kill)+2.5,y-2.5)
    c.line(x(resume),y-4,x(resume),y+4);c.drawString(x(resume)+3,y-3,'lost '+t['lost'])
c.setStrokeColorRGB(.3,.3,.3);c.setLineWidth(.8);c.line(x(0),32,x(17),32)
for t in (0,5,10,15):
    c.line(x(t),32,x(t),29);c.drawCentredString(x(t),19,str(t))
c.drawCentredString(126,5,'Time since replica disconnection (s)')
c.save()
if shutil.which('pdftoppm'):  # Poppler; only needed for the PNG copy
    subprocess.run(['pdftoppm','-png','-r','300','-singlefile',str(path),str(path.with_suffix(''))],check=True)
print(path)
