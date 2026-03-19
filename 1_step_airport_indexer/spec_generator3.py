import os, re, json
root = r'C:\Users\Ravery\Desktop\AIRPORTS'
index = os.path.join(root, 'CODE', 'INDEX', 'MASTER_TILE_INDEX_AIRPORTS.rtf')
out_dir = os.path.join(root, 'CODE', 'brcj_pgw_geotiff', 'specs')
os.makedirs(out_dir, exist_ok=True)

airports = ['KASE','KCRQ','KMWL','KMYF','KSAN','KDAL','KDFW','KJFK','KLGA']
merc_map = {'Z13':9,'Z14':9,'Z15':9,'Z16':11,'Z17':13,'Z18':13}
coord_pat = re.compile(r"\((\d+),(\d+)\)")
with open(index,'r',encoding='utf-8') as fh:
    data = fh.read()

# get available source zooms
src_zoom_map = {}
for ap in airports:
    brcj = os.path.join(root, ap + '_BRCJ')
    z = None
    if os.path.isdir(brcj):
        zooms = [int(d) for d in os.listdir(brcj) if d.isdigit()]
        if zooms:
            z = max(zooms)
    src_zoom_map[ap] = z if z else 18

created = []
for ap in airports:
    i = data.find(ap + '\\')
    if i == -1:
        print(f"{ap}: not found in index")
        continue
    snippet = data[i:i+1200]
    spec = {'zoom': src_zoom_map[ap], 'merc_zoom': None, 'tile_size':256, 'groups':[]}
    matches = re.findall(r"(Z\d{2} .*?;\s*CENTER .*?)\\\\", snippet, flags=re.S)
    if not matches:
        print(f"{ap}: no Z lines found in snippet")
        continue
    gi = 0
    for m in matches:
        zlabel = re.match(r"Z\d{2}", m).group(0)
        merc = merc_map.get(zlabel)
        spec['merc_zoom'] = merc
        parts = [p.strip() for p in re.split(r"\|", m) if p.strip()]
        for part in parts:
            coords = coord_pat.findall(part)
            if not coords: continue
            gi += 1
            files = []
            scale = 2 ** (spec['zoom'] - merc)
            for mx,my in coords:
                mx=int(mx); my=int(my)
                sx0 = mx * scale
                sy0 = my * scale
                for sx in range(sx0, sx0+scale):
                    for sy in range(sy0, sy0+scale):
                        files.append(f"{sx}_{sy}.png")
            group_name = f"{ap}_M{merc}_G{gi}"
            spec['groups'].append({'name': group_name, 'files': files, 'output_tiff': f"{group_name}.tif"})
    if spec['groups']:
        out_path = os.path.join(out_dir, f"{ap}_spec.json")
        with open(out_path,'w',encoding='utf-8') as fh:
            json.dump(spec, fh, indent=2)
        created.append(out_path)
        print(f"Wrote {out_path}")
    else:
        print(f"{ap}: no groups parsed")

print(f"Created {len(created)} specs")
