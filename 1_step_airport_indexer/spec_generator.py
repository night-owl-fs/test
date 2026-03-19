import re, os, json

root = r'C:\Users\Ravery\Desktop\AIRPORTS'
index_path = os.path.join(root, 'CODE', 'INDEX', 'MASTER_TILE_INDEX_AIRPORTS.rtf')
out_dir = os.path.join(root, 'CODE', 'brcj_pgw_geotiff', 'specs')
os.makedirs(out_dir, exist_ok=True)

# merc zoom mapping per top note
merc_map = {'Z13':9, 'Z14':9, 'Z15':9, 'Z16':11, 'Z17':13, 'Z18':13}

text = open(index_path, 'r', encoding='utf-8').read()
# split by airport blocks using separator lines of =====
blocks = re.split(r'=+\\n', text)

# find airport blocks by name lines like "KASE\"
airport_blocks = []
for b in blocks:
    m = re.search(r"^([A-Z0-9]{3,4})\\\\", b, flags=re.M)
    if m:
        airport_blocks.append(b)

# helper to extract tuples from a Z line
coord_pat = re.compile(r"\((\d+),(\d+)\)")

for b in airport_blocks:
    # get airport name
    name_m = re.search(r"^([A-Z0-9]{3,4})\\\\", b, flags=re.M)
    if not name_m: continue
    ap = name_m.group(1).strip()

    # determine source zoom (max) from existing BRCJ folder
    brcj_dir = os.path.join(root, ap + '_BRCJ')
    src_zoom = None
    if os.path.isdir(brcj_dir):
        zooms = [int(d) for d in os.listdir(brcj_dir) if d.isdigit()]
        if zooms:
            src_zoom = max(zooms)
    if not src_zoom:
        src_zoom = 18

    spec = {
        'zoom': src_zoom,
        'merc_zoom': None,
        'tile_size': 256,
        'groups': []
    }

    # find all Z lines inside block
    for zm in ['Z13','Z14','Z15','Z16','Z17','Z18']:
        mline = re.search(rf"{zm}.*?;\s*CENTER.*?\\n", b, flags=re.S)
        if not mline:
            continue
        line = mline.group(0)
        merc = merc_map.get(zm)
        spec['merc_zoom'] = merc

        # split groups by '|'
        parts = [p.strip() for p in re.split(r"\|", line) if p.strip()]
        group_idx = 0
        for part in parts:
            coords = coord_pat.findall(part)
            if not coords:
                continue
            # coords is list of (mx,my) in merc_zoom
            # expand to source tiles at src_zoom
            scale = 2 ** (src_zoom - merc)
            files = []
            for mx,my in coords:
                mx = int(mx); my = int(my)
                sx0 = mx * scale
                sy0 = my * scale
                for sx in range(sx0, sx0 + scale):
                    for sy in range(sy0, sy0 + scale):
                        files.append(f"{sx}_{sy}.png")
            group_idx += 1
            group_name = f"{ap}_M{merc}_G{group_idx}"
            out_tiff = f"{ap}_M{merc}_G{group_idx}.tif"
            spec['groups'].append({'name': group_name, 'files': files, 'output_tiff': out_tiff})

    # write spec if we have groups
    if spec['groups']:
        out_path = os.path.join(out_dir, f"{ap}_spec.json")
        with open(out_path, 'w', encoding='utf-8') as fh:
            json.dump(spec, fh, indent=2)
        print(f"Wrote spec: {out_path} (groups={len(spec['groups'])}, zoom={spec['zoom']}, merc_zoom={spec['merc_zoom']})")
    else:
        print(f"No groups parsed for {ap}")
