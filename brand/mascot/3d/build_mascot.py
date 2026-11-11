"""Builds the SprigDB axolotl mascot in Blender: model, rig, animations, exports.

Run headless:
    blender -b --factory-startup -P build_mascot.py -- [--render]

Writes, next to this script:
    sprig-mascot.blend   editable source (model, rig, actions)
    sprig-mascot.glb     glTF 2.0 binary for the web / Studio (three.js, Babylon, model-viewer)
    previews/            turnaround and one GIF per animation (with --render)

The model is chibi game-art: teal body, lime belly, leaf gills, a sprig on top, a brass
leaf-button eye, a star patch and a bandage, with black inverted-hull outlines. Parts are
rigid and bone-parented (no weight painting needed); only the tail is skinned.

Axes: Blender is Z-up and the mascot faces -Y. The glTF export converts to Y-up.
"""
import math
import os
import sys

import bmesh
import bpy
from mathutils import noise
from mathutils import Matrix, Quaternion, Vector

HERE = os.path.dirname(os.path.abspath(__file__))
ARGS = sys.argv[sys.argv.index('--') + 1:] if '--' in sys.argv else []
RENDER = '--render' in ARGS

# ---------------------------------------------------------------- palette (sRGB hex)
PALETTE = {
    'body': '#35C2B5', 'body_dark': '#1FA396', 'belly': '#B9DC3A', 'leaf': '#A3D13A',
    'leaf_dark': '#7FB02A', 'stem': '#7A4A2A', 'outline': '#0B1A1E', 'eye': '#0B1A1E',
    'white': '#FFFFFF', 'coin': '#F2AE1C', 'coin_ring': '#D9930F', 'coin_face': '#F7BE3A',
    'emblem': '#2E7D32', 'cheek': '#F08A6A', 'mouth': '#5E1A1E', 'tongue': '#F07A6A',
    'star': '#FFD43B', 'bandage': '#F3E7C9', 'bandage_line': '#8C7650', 'vein': '#5E9A1E', 'vein_teal': '#14786F',
    'spine': '#A3D13A', 'spine_soft': '#6CC98A',
}


def lin(hexcol):
    h = hexcol.lstrip('#')
    c = [int(h[i:i + 2], 16) / 255 for i in (0, 2, 4)]
    return tuple(x / 12.92 if x <= 0.04045 else ((x + 0.055) / 1.055) ** 2.4 for x in c) + (1.0,)


# ---------------------------------------------------------------- scene reset
bpy.ops.wm.read_factory_settings(use_empty=True)
scene = bpy.context.scene
coll = scene.collection
scene.render.fps = 24


def material(name, hexcol, rough=0.7, cull=False):
    m = bpy.data.materials.new(name)
    try:
        m.use_nodes = True
    except Exception:  # Blender 5 always uses nodes
        pass
    bsdf = m.node_tree.nodes.get('Principled BSDF')
    bsdf.inputs['Base Color'].default_value = lin(hexcol)
    bsdf.inputs['Roughness'].default_value = rough
    for key in ('Specular IOR Level', 'Specular'):
        if key in bsdf.inputs:
            bsdf.inputs[key].default_value = 0.25
    m.diffuse_color = lin(hexcol)
    m.use_backface_culling = cull
    return m


MAT = {k: material(k, v) for k, v in PALETTE.items()}
MAT['outline'] = material('outline', PALETTE['outline'], rough=1.0, cull=True)
MAT['skin'] = material('skin', PALETTE['body'])
_attr = MAT['skin'].node_tree.nodes.new('ShaderNodeVertexColor')
_attr.layer_name = 'skin'
MAT['skin'].node_tree.links.new(_attr.outputs['Color'],
                                MAT['skin'].node_tree.nodes['Principled BSDF'].inputs['Base Color'])


def _mix(a, b, t):
    t = max(0.0, min(1.0, t))
    return tuple(x + (y - x) * t for x, y in zip(a, b))


def _smooth(e0, e1, x):
    t = max(0.0, min(1.0, (x - e0) / (e1 - e0)))
    return t * t * (3 - 2 * t)


SKIN_PALE, SKIN_BASE, SKIN_DEEP = lin('#7FDDD2'), lin('#38C6B8'), lin('#1E968B')
SPOT_DARK, SPOT_LIME, WASH = lin('#178479'), lin('#9BD43F'), lin('#5FD0C4')


def skin_colour(p):
    """Watercolour axolotl skin at world point p, after Kiran's references, in SprigDB teal:
    pale face and front, colour deepening towards the back and the extremities,
    soft washes, and crisp round spots of mixed sizes over the head top and back."""
    # pale at the front of the face and chest, base teal elsewhere
    front = _smooth(0.1, -0.45, p.y) * (0.6 + 0.4 * _smooth(0.6, 1.1, p.z))
    c = _mix(SKIN_BASE, SKIN_PALE, 0.75 * front)
    # deepen towards the back, the limbs' ends and the tail
    ext = max(_smooth(0.55, 0.85, abs(p.x)) * _smooth(1.0, 0.6, p.z),   # hands, feet, arm ends
              _smooth(0.18, 0.02, p.z),                                  # soles and toes
              _smooth(0.55, 1.25, p.y))                                  # tail
    c = _mix(c, SKIN_DEEP, 0.55 * ext + 0.3 * _smooth(0.0, 0.5, p.y))
    # soft watercolour washes
    w = noise.noise(p * 3.2 + Vector((7.0, 3.0, 1.0)))
    c = _mix(c, WASH, 0.35 * _smooth(0.1, 0.5, w))
    # crisp round spots, mostly on the head top and the back, never on the face front
    region = max(_smooth(1.38, 1.6, p.z), _smooth(-0.05, 0.25, p.y)) * _smooth(-0.35, -0.2, p.y)
    if region > 0.01:
        d, pts = noise.voronoi(p * 4.5, distance_metric="DISTANCE")
        k = noise.noise(pts[0] * 17.3)            # per-spot size and colour
        r = 0.22 + 0.22 * (k + 1) / 2
        if (k + 1) / 2 > 0.15:                    # some cells have no spot
            spot = _smooth(r, r * 0.75, d[0]) * region
            c = _mix(c, SPOT_LIME if k > 0.55 else SPOT_DARK, spot)
    return c
for k in ('coin', 'coin_face', 'coin_ring'):
    MAT[k].node_tree.nodes['Principled BSDF'].inputs['Metallic'].default_value = 0.35
    MAT[k].node_tree.nodes['Principled BSDF'].inputs['Roughness'].default_value = 0.35


# ---------------------------------------------------------------- geometry helpers
def orient(d, n):
    """Rotation taking local Z to d and local Y towards n."""
    z = Vector(d).normalized()
    y = Vector(n) - Vector(n).dot(z) * z
    y.normalize()
    x = y.cross(z)
    return Matrix((x, y, z)).transposed().to_4x4()


def make_obj(name, bm, mat, outline=0.02, smooth=True, face_mat=None):
    """face_mat(centre, face) -> extra material key or None, for painted bands (spine, tail top)."""
    for f in bm.faces:
        f.smooth = smooth
    bmesh.ops.recalc_face_normals(bm, faces=bm.faces[:])
    if mat == 'body':
        mat = 'skin'
    keys = [mat]
    if face_mat:
        for f in bm.faces:
            k = face_mat(f.calc_center_median(), f)
            if k:
                if k not in keys:
                    keys.append(k)
                f.material_index = keys.index(k)
    me = bpy.data.meshes.new(name)
    bm.to_mesh(me)
    bm.free()
    if mat == 'skin':
        at = me.color_attributes.new(name='skin', type='BYTE_COLOR', domain='POINT')
        for v in me.vertices:
            at.data[v.index].color = skin_colour(v.co)
    ob = bpy.data.objects.new(name, me)
    coll.objects.link(ob)
    for k in keys:
        me.materials.append(MAT[k])
    if outline:
        for _ in keys:  # every slot i maps to i + len(keys) = outline
            me.materials.append(MAT['outline'])
        mod = ob.modifiers.new('outline', 'SOLIDIFY')
        mod.thickness = outline * OUTLINE_K
        mod.offset = 1.0
        mod.use_flip_normals = True
        mod.use_rim = False
        mod.material_offset = len(keys)
    return ob


def ellipsoid(name, center, scale, mat, rot=Matrix(), outline=0.02, segs=48, rings=24, boxy=1.0,
              face_mat=None, half=None, taper=0.0):
    """boxy < 1 gives a rounded box (superellipsoid); taper > 0 widens the bottom and narrows the top
    (a trapezoid head, a pear body); half='bottom' keeps the lower half, capped."""
    bm = bmesh.new()
    bmesh.ops.create_uvsphere(bm, u_segments=segs, v_segments=rings, radius=1.0)
    if boxy != 1.0:
        for v in bm.verts:
            v.co = Vector(math.copysign(abs(c) ** boxy, c) for c in v.co)
    if taper:
        for v in bm.verts:
            k = 1 - taper * v.co.z
            v.co.x *= k
            v.co.y *= k if name != 'head' else 1.0
    if half:
        bmesh.ops.bisect_plane(bm, geom=bm.verts[:] + bm.edges[:] + bm.faces[:], plane_co=(0, 0, 0),
                               plane_no=(0, 0, 1 if half == 'bottom' else -1), clear_outer=True)
        bmesh.ops.contextual_create(bm, geom=[e for e in bm.edges if e.is_boundary])
    m = Matrix.Translation(center) @ rot @ Matrix.Diagonal(Vector(scale).to_4d())
    bmesh.ops.transform(bm, matrix=m, verts=bm.verts)
    return make_obj(name, bm, mat, outline, face_mat=face_mat)


def limb(name, a, b, r, mat, outline=0.02):
    a, b = Vector(a), Vector(b)
    half = (b - a).length / 2 + r
    rot = orient(b - a, (0, -1, 0))
    return ellipsoid(name, (a + b) / 2, (r, r, half), mat, rot, outline)


VEINS = []  # (vein object, leaf name): parented to the leaf's bone later
VEIN_MAT = {'leaf': 'vein', 'body_dark': 'vein_teal'}


def leaf(name, base, d, n, length, width, mat, thick=0.035, outline=0.015, vein=True, curl=0.0):
    """curl < 0 bends the leaf away from its front face along its length."""
    m = Matrix.Translation(base) @ orient(d, n)
    bm = bmesh.new()
    bmesh.ops.create_uvsphere(bm, u_segments=20, v_segments=14, radius=1.0)
    for v in bm.verts:
        x, y, z = v.co
        k = math.sqrt(max(0.0, 1 - z * z))
        s = (z + 1) / 2
        v.co = Vector((x * width * (0.35 + 0.65 * k), y * thick + curl * length * s * s, s * length))
    bmesh.ops.transform(bm, matrix=m, verts=bm.verts)
    ob = make_obj(name, bm, mat, outline)
    if vein:
        # the midrib: a thin tapering ribbon lying on both faces of the leaf, from base to near the tip
        vb = bmesh.new()
        for side in (1, -1):
            rows = []
            for i in range(13):
                s = 0.04 + 0.84 * i / 12
                z = 2 * s - 1
                k = math.sqrt(max(0.0, 1 - z * z))
                y = side * (thick * k + 0.004) + curl * length * s * s
                w = width * 0.11 * (1 - 0.7 * s)
                rows.append((vb.verts.new((-w, y, s * length)), vb.verts.new((w, y, s * length))))
            for (a0, a1), (b0, b1) in zip(rows, rows[1:]):
                vb.faces.new((a0, a1, b1, b0))
        bmesh.ops.transform(vb, matrix=m, verts=vb.verts)
        VEINS.append((make_obj(name + '_vein', vb, VEIN_MAT.get(mat, 'vein'), outline=0), name))
    return ob


def cylinder(name, center, axis, radius, depth, mat, outline=0.012, caps=True, segs=40, smooth=True):
    bm = bmesh.new()
    bmesh.ops.create_cone(bm, cap_ends=caps, cap_tris=False, segments=segs,
                          radius1=radius, radius2=radius, depth=depth)
    up = (0, 0, 1) if abs(Vector(axis).normalized().z) < 0.9 else (0, -1, 0)
    bmesh.ops.transform(bm, matrix=Matrix.Translation(center) @ orient(axis, up), verts=bm.verts)
    ob = make_obj(name, bm, mat, outline, smooth=smooth)
    return ob


def star(name, center, outer, inner, depth, mat, tilt=0.0):
    bm = bmesh.new()
    pts = []
    for i in range(10):
        a = math.pi / 2 + i * math.pi / 5 + tilt
        r = outer if i % 2 == 0 else inner
        pts.append(bm.verts.new((r * math.cos(a), 0.0, r * math.sin(a))))
    face = bm.faces.new(pts)
    ext = bmesh.ops.extrude_face_region(bm, geom=[face])
    moved = [g for g in ext['geom'] if isinstance(g, bmesh.types.BMVert)]
    bmesh.ops.translate(bm, vec=(0, depth, 0), verts=moved)
    bmesh.ops.translate(bm, vec=center, verts=bm.verts)
    return make_obj(name, bm, mat, outline=0.008, smooth=False)


# ---------------------------------------------------------------- the model
P = {}  # part name -> (object, bone)
OUTLINE_K = 1.7  # heavy game-art outlines

# a lime band runs down the spine and along the tail top
def spine_band(core, soft, y_min, z_min=-9.0, z_max=9.0):
    def pick(c, _face):
        if c.y < y_min or not (z_min < c.z < z_max):
            return None
        if abs(c.x) < core:
            return 'spine'
        if abs(c.x) < soft:
            return 'spine_soft'
        return None
    return pick


# body: pear-shaped, wider down by the legs (like the art)
P['body'] = (ellipsoid('body', (0, 0.03, 0.64), (0.52, 0.44, 0.62), 'body', outline=0.025, segs=128, rings=64,
                       taper=0.28, face_mat=spine_band(0.06, 0.12, 0.12, z_min=0.3)), 'body')
P['belly'] = (ellipsoid('belly', (0, -0.18, 0.60), (0.39, 0.29, 0.50), 'belly', outline=0.012, taper=0.25), 'body')
P['star'] = (star('star', (0.13, -0.47, 0.84), 0.08, 0.035, 0.04, 'star'), 'body')

# head: a trapezoid-ish rounded box, wide at the cheeks and narrower at the top, like the art
HEAD_C, HEAD_S, HEAD_P, HEAD_T = Vector((0, 0, 1.25)), (0.84, 0.60, 0.593), 0.72, 0.16


def head_front(x, z):
    """y of the head's front surface at (x, z)."""
    a, b, c = HEAD_S
    zu = (z - HEAD_C.z) / c
    xu = x / (a * (1 - HEAD_T * zu))
    q = 1 - abs(xu) ** (2 / HEAD_P) - abs(zu) ** (2 / HEAD_P)
    return HEAD_C.y - b * max(q, 0.0) ** (HEAD_P / 2)


def on_face(x, z, out=0.0):
    """A point on the face at (x, z), pushed `out` towards the viewer (negative = sunk in)."""
    return (x, head_front(x, z) - out, z)


def stroke(name, pts, width, mat, out=0.006):
    """A thin ribbon lying on the face through (x, z) points: drawn lines like the forehead crease."""
    bm = bmesh.new()
    rows = []
    for i, (x, z) in enumerate(pts):
        p = Vector(on_face(x, z, out))
        nx, nz = (pts[min(i + 1, len(pts) - 1)][0] - pts[max(i - 1, 0)][0],
                  pts[min(i + 1, len(pts) - 1)][1] - pts[max(i - 1, 0)][1])
        side = Vector((-nz, 0, nx)).normalized() * width / 2
        rows.append((bm.verts.new(p - side), bm.verts.new(p + side)))
    for (a0, a1), (b0, b1) in zip(rows, rows[1:]):
        bm.faces.new((a0, a1, b1, b0))
    return make_obj(name, bm, mat, outline=0)


def head_band(c, _face):
    """The spine's lime band, continued up the back of the head and over the top to the crease."""
    if not (c.y > 0.1 or (c.z > 1.66 and c.y > -0.40)):
        return None
    if c.z < 0.9:
        return None
    if abs(c.x) < 0.05:
        return 'spine'
    if abs(c.x) < 0.10:
        return 'spine_soft'
    return None


P['head'] = (ellipsoid('head', HEAD_C, HEAD_S, 'body', outline=0.028, segs=160, rings=80, boxy=HEAD_P,
                       taper=HEAD_T, face_mat=head_band), 'head')
# the band ends at the crease in two small leaf buds
for side, sx in (('L', 1), ('R', -1)):
    P[f'bud_{side}'] = (leaf(f'bud_{side}', on_face(sx * 0.02, 1.80, -0.01), (sx * 0.75, -0.35, 0.6),
                             (0, -1, 0.35), 0.13, 0.065, 'leaf', thick=0.022, outline=0.008), 'head')
# the forehead mark: a "V" pointing down from the top of the head
P['crease'] = (stroke('crease', [(-0.13, 1.77), (-0.065, 1.69), (0.0, 1.60), (0.065, 1.69), (0.13, 1.77)],
                      0.022, 'vein_teal'), 'head')

# snout: a prominent muzzle bulge carrying the nostrils and the mouth
SN = 0.07  # how far the muzzle stands out of the face
P['snout'] = (ellipsoid('snout', on_face(0, 1.13, -0.05), (0.34, 0.12, 0.17), 'body', outline=0.014), 'head')
P['cheek_R'] = (ellipsoid('cheek_R', on_face(-0.56, 1.06), (0.10, 0.025, 0.055), 'cheek', outline=0), 'head')
P['cheek_L'] = (ellipsoid('cheek_L', on_face(0.60, 1.04), (0.09, 0.025, 0.05), 'cheek', outline=0), 'head')
for side, x in (('R', -0.065), ('L', 0.065)):
    P[f'nostril_{side}'] = (ellipsoid(f'nostril_{side}', on_face(x, 1.22, SN - 0.005), (0.024, 0.016, 0.02), 'eye',
                                      outline=0, segs=12, rings=8), 'head')

# mouth: a "D" (lower half of an ellipsoid) on the muzzle, a tongue, and two small rounded teeth;
# all hinge from the top lip on the `mouth` bone (scale it down to close the mouth into a smile line)
MOUTH_TOP = on_face(0, 1.12, SN)
P['mouth'] = (ellipsoid('mouth', on_face(0, 1.12, SN - 0.035), (0.22, 0.07, 0.14), 'mouth', outline=0.012,
                        half='bottom'), 'mouth')
P['tongue'] = (ellipsoid('tongue', on_face(0, 1.03, SN + 0.008), (0.11, 0.03, 0.045), 'tongue', outline=0), 'mouth')
for side, x in (('R', -0.10), ('L', 0.10)):
    P[f'tooth_{side}'] = (ellipsoid(f'tooth_{side}', on_face(x, 1.12, SN + 0.025), (0.034, 0.02, 0.04), 'white',
                                    outline=0.004, half='bottom', segs=16, rings=8), 'mouth')

# eye (character's right, viewer's left): big and black, with a highlight; blinks via its bone
EYE_C = on_face(-0.34, 1.34, -0.03)
P['eye'] = (ellipsoid('eye', EYE_C, (0.17, 0.10, 0.19), 'eye', outline=0), 'eye')
P['eye_hi'] = (ellipsoid('eye_hi', on_face(-0.29, 1.43, 0.085), (0.058, 0.022, 0.058), 'white', outline=0,
                         segs=16, rings=8), 'eye')

# brass leaf-button eye (character's left, viewer's right)
CX, CZ = 0.35, 1.34
CY = head_front(CX, CZ) - 0.04   # front face of the coin
P['coin'] = (cylinder('coin', (CX, CY + 0.04, CZ), (0, -1, 0), 0.215, 0.08, 'coin', outline=0.018,
                      smooth=False), 'head')
P['coin_ring'] = (cylinder('coin_ring', (CX, CY - 0.004, CZ), (0, -1, 0), 0.168, 0.012, 'coin_ring', outline=0,
                           smooth=False), 'head')
P['coin_face'] = (cylinder('coin_face', (CX, CY - 0.010, CZ), (0, -1, 0), 0.147, 0.012, 'coin_face', outline=0,
                           smooth=False), 'head')
# the emblem: a small sprig (stem and two leaves) standing proud of the coin face, outlined
EY = CY - 0.03
P['emblem_stem'] = (limb('emblem_stem', (CX - 0.06, EY, CZ - 0.10), (CX + 0.0, EY, CZ + 0.05), 0.014, 'emblem',
                         outline=0.004), 'head')
P['emblem'] = (leaf('emblem', (CX - 0.005, EY, CZ + 0.025), (0.75, 0, 0.65), (0, -1, 0), 0.135, 0.06, 'emblem',
                    thick=0.012, outline=0.005, vein=False), 'head')
P['emblem_2'] = (leaf('emblem_2', (CX - 0.025, EY, CZ - 0.012), (-0.8, 0, 0.6), (0, -1, 0), 0.11, 0.05, 'emblem',
                      thick=0.012, outline=0.005, vein=False), 'head')

# leaf gills: lime leaves in front, teal leaves behind, all swept back and curled, so they have depth
for side, sx in (('L', 1), ('R', -1)):
    bone = f'gills_{side}'
    for i, (z, dx, dy, dz) in enumerate(((1.52, 0.62, 0.35, 0.70), (1.30, 0.95, 0.40, 0.05),
                                         (1.08, 0.80, 0.30, -0.55))):
        x0 = 0.86 if i == 2 else 0.78
        P[f'gill_{side}{i}'] = (leaf(f'gill_{side}{i}', (sx * x0, 0.0, z), (sx * dx, dy, dz),
                                     (sx * 0.5, -1, 0), 0.46, 0.17, 'leaf', curl=-0.18), bone)
    for i, (z, dx, dy, dz) in enumerate(((1.42, 0.80, 0.80, 0.45), (1.20, 0.85, 0.80, -0.25))):
        P[f'gillb_{side}{i}'] = (leaf(f'gillb_{side}{i}', (sx * 0.78, 0.16, z), (sx * dx, dy, dz),
                                      (sx * 0.5, -1, 0), 0.54, 0.18, 'body_dark', curl=-0.15), bone)

# the sprig on top
P['stem'] = (limb('stem', (0, 0.0, 1.78), (0, 0.0, 2.06), 0.035, 'stem', outline=0.012), 'sprig')
P['sprig_L'] = (leaf('sprig_L', (0.02, 0, 2.04), (0.85, 0, 0.55), (0, -1, 0.25), 0.27, 0.12, 'leaf'), 'sprig')
P['sprig_R'] = (leaf('sprig_R', (-0.02, 0, 2.04), (-0.85, 0, 0.55), (0, -1, 0.25), 0.27, 0.12, 'leaf'), 'sprig')

# arms: chunky upper arm and forearm, big hand with three fingers, bandage on the left forearm
ARM = {'L': ((0.42, -0.10, 0.96), (0.62, -0.18, 0.72), (0.72, -0.27, 0.49)),
       'R': ((-0.42, -0.10, 0.96), (-0.62, -0.18, 0.72), (-0.72, -0.27, 0.49))}
for side, (sh, el, ha) in ARM.items():
    sh, el, ha = Vector(sh), Vector(el), Vector(ha)
    P[f'arm_{side}'] = (limb(f'arm_{side}', sh, el, 0.13, 'body'), f'arm_{side}')
    P[f'elbow_{side}'] = (ellipsoid(f'elbow_{side}', el, (0.125, 0.125, 0.125), 'body', segs=24, rings=12),
                          f'arm_{side}')
    P[f'fore_{side}'] = (limb(f'fore_{side}', el, ha, 0.12, 'body'), f'fore_{side}')
    P[f'hand_{side}'] = (ellipsoid(f'hand_{side}', ha, (0.145, 0.13, 0.13), 'body'), f'fore_{side}')
    d = (ha - el).normalized()
    s = d.cross(Vector((0, 1, 0))).normalized()
    for j, off in enumerate((-0.08, 0.0, 0.08)):
        c = ha + d * 0.095 + s * off + Vector((0, -0.03, 0))
        P[f'finger_{side}{j}'] = (ellipsoid(f'finger_{side}{j}', c, (0.054, 0.054, 0.07), 'body',
                                            rot=orient(d, (0, -1, 0)), outline=0.012, segs=16, rings=10),
                                  f'fore_{side}')
_, a, b = map(Vector, ARM['L'])
P['bandage'] = (cylinder('bandage', a.lerp(b, 0.5), b - a, 0.133, 0.13, 'bandage', outline=0.01, caps=False), 'fore_L')
# the wrap lines: two tilted rings crossing over the bandage
d = (b - a).normalized()
perp = d.cross(Vector((0, -1, 0))).normalized()
for j, ang in enumerate((24, -24)):
    ax = Matrix.Rotation(math.radians(ang), 3, perp) @ d
    P[f'bandage_line{j}'] = (cylinder(f'bandage_line{j}', a.lerp(b, 0.5), ax, 0.143, 0.02, 'bandage_line',
                                      outline=0, caps=False), 'fore_L')

# two legs: chunky thigh bent forward to a knee, a thick shin, a big flat foot with three toes; wide stance
LEGS = {'L': ((0.30, -0.02, 0.44), (0.38, -0.23, 0.28), (0.37, -0.10, 0.10)),
        'R': ((-0.30, -0.02, 0.44), (-0.38, -0.23, 0.28), (-0.37, -0.10, 0.10))}
for side, (hip, knee, ankle) in LEGS.items():
    hip, knee, ankle = Vector(hip), Vector(knee), Vector(ankle)
    P[f'thigh_{side}'] = (limb(f'thigh_{side}', hip, knee, 0.205, 'body'), f'leg_{side}')
    P[f'knee_{side}'] = (ellipsoid(f'knee_{side}', knee, (0.175, 0.175, 0.175), 'body', segs=24, rings=12),
                         f'shin_{side}')
    P[f'shin_{side}'] = (limb(f'shin_{side}', knee, ankle, 0.16, 'body'), f'shin_{side}')
    x = ankle.x
    P[f'foot_{side}'] = (ellipsoid(f'foot_{side}', (x, ankle.y - 0.08, 0.065), (0.19, 0.235, 0.085), 'body'),
                         f'shin_{side}')
    for j, dx in enumerate((-0.10, 0.0, 0.10)):
        P[f'toe_{side}{j}'] = (ellipsoid(f'toe_{side}{j}', (x + dx, ankle.y - 0.28, 0.055), (0.064, 0.074, 0.056),
                                         'body', outline=0.012, segs=16, rings=10), f'shin_{side}')

# tail: a flat, tapering paddle along a curve, skinned to three bones; lime along the top and tip
T0, T1, T2 = Vector((0, 0.30, 0.34)), Vector((0, 0.95, 0.16)), Vector((0, 1.32, 0.60))


def bez(t):
    return (1 - t) ** 2 * T0 + 2 * (1 - t) * t * T1 + t * t * T2


def bez_tan(t):
    return (2 * (1 - t) * (T1 - T0) + 2 * t * (T2 - T1)).normalized()


bm = bmesh.new()
bmesh.ops.create_uvsphere(bm, u_segments=40, v_segments=64, radius=1.0)
tail_t, tail_w = {}, {}
for v in bm.verts:
    x, y, z = v.co
    t = (z + 1) / 2
    e = math.sqrt(max(0.0, 1 - z * z))
    rr = math.hypot(x, y) or 1.0
    u, w = x / rr, y / rr
    width = 0.12 * (1 - 0.6 * t) * e ** 0.55
    height = 0.25 * (1 - 0.45 * t) * e ** 0.55
    tan = bez_tan(t)
    nrm = Vector((0, -tan.z, tan.y))  # perpendicular in the YZ plane, pointing up
    v.co = bez(t) + Vector((u * width, 0, 0)) + nrm * (w * height)
    tail_t[v.index], tail_w[v.index] = t, w


def tail_band(_c, face):
    w = sum(tail_w[v.index] for v in face.verts) / len(face.verts)
    t = sum(tail_t[v.index] for v in face.verts) / len(face.verts)
    if w > 0.62 and t > 0.08:
        return 'spine'
    if (w > 0.25 and t > 0.05) or t > 0.84:
        return 'spine_soft'
    return None


tail_obj = make_obj('tail', bm, 'body', outline=0.022, face_mat=tail_band)

# leaf veins follow their leaf's bone
for vein, owner in VEINS:
    P[vein.name] = (vein, P[owner][1])

# the head group sits on the taller body
HEAD_LIFT = 0.14
HEAD_BONES = ('head', 'eye', 'mouth', 'gills_L', 'gills_R', 'sprig')
for ob, bone in P.values():
    if bone in HEAD_BONES:
        ob.data.transform(Matrix.Translation((0, 0, HEAD_LIFT)))

# ---------------------------------------------------------------- the rig
arm_data = bpy.data.armatures.new('SprigRig')
rig = bpy.data.objects.new('SprigRig', arm_data)
coll.objects.link(rig)
bpy.context.view_layer.objects.active = rig
bpy.ops.object.mode_set(mode='EDIT')
eb = arm_data.edit_bones

BONES = [
    # name, head, tail, parent
    ('root', (0, 0, 0), (0, 0, 0.3), None),
    ('body', (0, 0, 0.3), (0, 0, 0.95), 'root'),
    ('head', (0, 0, 0.95), (0, 0, 1.75), 'body'),
    ('sprig', (0, 0, 1.84), (0, 0, 2.08), 'head'),
    ('eye', EYE_C, (EYE_C[0], EYE_C[1], EYE_C[2] + 0.14), 'head'),
    ('mouth', MOUTH_TOP, (MOUTH_TOP[0], MOUTH_TOP[1], MOUTH_TOP[2] + 0.1), 'head'),
    ('gills_L', (0.80, 0.05, 1.30), (1.08, 0.05, 1.30), 'head'),
    ('gills_R', (-0.80, 0.05, 1.30), (-1.08, 0.05, 1.30), 'head'),
    ('arm_L', ARM['L'][0], ARM['L'][1], 'body'),
    ('fore_L', ARM['L'][1], ARM['L'][2], 'arm_L'),
    ('arm_R', ARM['R'][0], ARM['R'][1], 'body'),
    ('fore_R', ARM['R'][1], ARM['R'][2], 'arm_R'),
    ('tail_1', tuple(bez(0.0)), tuple(bez(1 / 3)), 'body'),
    ('tail_2', tuple(bez(1 / 3)), tuple(bez(2 / 3)), 'tail_1'),
    ('tail_3', tuple(bez(2 / 3)), tuple(bez(1.0)), 'tail_2'),
] + [b for n, (h, k, a) in LEGS.items() for b in ((f'leg_{n}', h, k, 'root'), (f'shin_{n}', k, a, f'leg_{n}'))]
for name, h, t, parent in BONES:
    b = eb.new(name)
    b.head, b.tail = h, t
    if name in HEAD_BONES:
        b.head.z += HEAD_LIFT
        b.tail.z += HEAD_LIFT
    if parent:
        b.parent = eb[parent]
        b.use_connect = name.startswith('tail_') and parent.startswith('tail_')
bpy.ops.object.mode_set(mode='OBJECT')

# rigid parts: parent each object to its bone, keeping its world placement
for name, (ob, bone) in P.items():
    ob.parent = rig
    ob.parent_type = 'BONE'
    ob.parent_bone = bone
    bpy.context.view_layer.update()
    ob.matrix_world = Matrix.Identity(4)

# tail: skinned with linear weights along the curve
for i in range(3):
    tail_obj.vertex_groups.new(name=f'tail_{i + 1}')
for vi, t in tail_t.items():
    centres = [1 / 6, 1 / 2, 5 / 6]
    ws = [max(0.0, 1 - abs(t - c) * 3) for c in centres]
    if t <= centres[0]:
        ws = [1, 0, 0]
    if t >= centres[2]:
        ws = [0, 0, 1]
    s = sum(ws) or 1
    for i, w in enumerate(ws):
        if w > 0:
            tail_obj.vertex_groups[f'tail_{i + 1}'].add([vi], w / s, 'REPLACE')
tail_obj.parent = rig
mod = tail_obj.modifiers.new('rig', 'ARMATURE')
mod.object = rig
bpy.ops.object.select_all(action='DESELECT')
tail_obj.modifiers.move(tail_obj.modifiers.find('rig'), 0)

# ---------------------------------------------------------------- animation helpers
for pb in rig.pose.bones:
    pb.rotation_mode = 'QUATERNION'


def rest3(name):
    return arm_data.bones[name].matrix_local.to_3x3()


def world_rot(name, axis, deg):
    """Bone-local quaternion for a rotation about a world axis (through the bone head)."""
    m = rest3(name)
    rw = Quaternion(Vector(axis).normalized(), math.radians(deg)).to_matrix()
    return (m.inverted() @ rw @ m).to_quaternion()


def world_loc(name, vec):
    return rest3(name).inverted() @ Vector(vec)


def aim(name, target, k=1.0):
    """Bone-local quaternion pointing the bone along a world direction (k blends from rest)."""
    bone = arm_data.bones[name]
    rest = (bone.tail_local - bone.head_local).normalized()
    rw = rest.rotation_difference(Vector(target).normalized()).to_matrix()
    m = rest3(name)
    return Quaternion().slerp((m.inverted() @ rw @ m).to_quaternion(), k)


CHANNELS = ('location', 'rotation_quaternion', 'scale')


def pose(frame, spec):
    """spec: {bone: {'aim': (dir, k), 'rot': [(axis, deg), ...], 'loc': world vec, 'scale': local}}"""
    W = {}  # bone -> its posed rotation in armature space, so children can aim in world terms
    for pb in rig.pose.bones:  # parents come before children
        s = spec.get(pb.name, {})
        rp = W[pb.parent.name] if pb.parent else Matrix.Identity(3)
        r = Matrix.Identity(3)
        if 'aim' in s:
            target, k = s['aim']
            bone = arm_data.bones[pb.name]
            rest = (bone.tail_local - bone.head_local).normalized()
            full = rest.rotation_difference(rp.inverted() @ Vector(target).normalized())
            r = Quaternion().slerp(full, k).to_matrix()
        for axis, deg in s.get('rot', []):
            r = Quaternion(Vector(axis).normalized(), math.radians(deg)).to_matrix() @ r
        W[pb.name] = rp @ r
        m = rest3(pb.name)
        pb.rotation_quaternion = (m.inverted() @ r @ m).to_quaternion()
        pb.location = world_loc(pb.name, s.get('loc', (0, 0, 0)))
        pb.scale = s.get('scale', (1, 1, 1))
        for ch in CHANNELS:
            pb.keyframe_insert(ch, frame=frame)


X, Y, Z = (1, 0, 0), (0, 1, 0), (0, 0, 1)
BLINK = {'scale': (1, 0.08, 1)}


def idle(f):
    """48-frame loop: breathe, sway gills and tail, blink once."""
    p = math.sin(2 * math.pi * f / 48)
    spec = {
        'body': {'loc': (0, 0, 0.018 * p), 'scale': (1 + 0.012 * p, 1 - 0.01 * p, 1 + 0.012 * p)},
        'mouth': {'scale': (1, 0.15 + 0.35 * max(0.0, p), 1)},
        'head': {'rot': [(Y, 2.5 * p)]},
        'gills_L': {'rot': [(Y, -6 * p)]}, 'gills_R': {'rot': [(Y, 6 * p)]},
        'sprig': {'rot': [(Y, -5 * p)]},
        'tail_1': {'rot': [(Z, 6 * p)]}, 'tail_2': {'rot': [(Z, 8 * p)]}, 'tail_3': {'rot': [(Z, 10 * p)]},
    }
    if f in (34, 36):
        spec['eye'] = BLINK
    return spec


def wave(f):
    """48-frame loop: right arm up and waving, head tilted, tail wagging."""
    p = math.sin(2 * math.pi * f / 24)
    q = math.sin(2 * math.pi * f / 48)
    return {
        'arm_R': {'aim': ((-0.97, -0.25, -0.05), 1.0)},
        'fore_R': {'aim': ((-0.55, -0.4, 0.73), 1.0), 'rot': [(Y, 16 * p)]},
        'arm_L': {'rot': [(Y, 8)]},
        'mouth': {'scale': (1, 0.3 + 0.75 * abs(math.sin(2 * math.pi * f / 12)), 1)},
        'head': {'rot': [(Y, -6 + 2 * q)]},
        'body': {'rot': [(Y, -3)]},
        'gills_L': {'rot': [(Y, -8 * p)]}, 'gills_R': {'rot': [(Y, 8 * p)]},
        'sprig': {'rot': [(Y, 6 * p)]},
        'tail_1': {'rot': [(Z, 10 * q)]}, 'tail_2': {'rot': [(Z, 14 * q)]}, 'tail_3': {'rot': [(Z, 18 * q)]},
    }


def celebrate(f):
    """36-frame loop: crouch, jump with both arms up and eyes squeezed shut, land."""
    t = f / 36
    if t < 0.2:      # crouch
        k = t / 0.2; h = 0; sq = 0.1 * k; arms = 20 * k
    elif t < 0.65:   # airborne
        k = (t - 0.2) / 0.45; h = 0.34 * math.sin(math.pi * k); sq = -0.06 * math.sin(math.pi * k); arms = 135
    else:            # land and settle
        k = (t - 0.65) / 0.35; h = 0; sq = 0.1 * math.sin(math.pi * k); arms = 135 * (1 - k) + 20 * k * (1 - k)
    return {
        'root': {'loc': (0, 0, h), 'scale': (1 + sq * 0.6, 1 - sq, 1 + sq * 0.6)},
        'arm_R': {'aim': ((-0.85, -0.45, 0.3), arms / 135)}, 'arm_L': {'aim': ((0.85, -0.45, 0.3), arms / 135)},
        'fore_R': {'aim': ((-0.25, -0.6, 0.75), arms / 135)}, 'fore_L': {'aim': ((0.25, -0.6, 0.75), arms / 135)},
        'mouth': {'scale': (1, 1.15 if h > 0 else 0.5, 1)},
        'head': {'rot': [(X, -6 if h > 0 else 0)]},
        'gills_L': {'rot': [(Y, 14 if h > 0 else -4)]}, 'gills_R': {'rot': [(Y, -14 if h > 0 else 4)]},
        'sprig': {'rot': [(Y, 8 * math.sin(2 * math.pi * t * 2))]},
        'eye': {'scale': (1.1, 0.12, 1)},
        'tail_1': {'rot': [(X, -15 if h > 0 else 0)]}, 'tail_2': {'rot': [(X, -12 if h > 0 else 0)]},
    }


def sleep(f):
    """96-frame loop: head drooped, eye shut, slow deep breathing, gills hanging."""
    p = math.sin(2 * math.pi * f / 96)
    return {
        'body': {'rot': [(X, 8)], 'scale': (1 + 0.03 * p, 1 - 0.02 * p, 1 + 0.03 * p)},
        'mouth': {'scale': (1, 0.1 + 0.25 * max(0.0, p), 1)},
        'head': {'rot': [(X, 16 + 2 * p), (Y, 10)]},
        'eye': BLINK,
        'gills_L': {'rot': [(Y, -22 + 3 * p)]}, 'gills_R': {'rot': [(Y, 22 - 3 * p)]},
        'sprig': {'rot': [(Y, 18)]},
        'arm_L': {'rot': [(Y, -10)]}, 'arm_R': {'rot': [(Y, 10)]},
        'tail_1': {'rot': [(Z, 20)]}, 'tail_2': {'rot': [(Z, 25)]}, 'tail_3': {'rot': [(Z, 25)]},
    }


def sad(f):
    """48-frame loop: slumped, head down, gills and sprig drooping, small sniffle."""
    p = math.sin(2 * math.pi * f / 48)
    sniff = 0.015 if f in (20, 22, 26) else 0
    return {
        'body': {'rot': [(X, 6)], 'loc': (0, 0, -0.02 - sniff)},
        'mouth': {'scale': (1, 0.1, 1)},
        'head': {'rot': [(X, 12 + 1.5 * p)]},
        'gills_L': {'rot': [(Y, -30)]}, 'gills_R': {'rot': [(Y, 30)]},
        'sprig': {'rot': [(Y, 28)]},
        'arm_L': {'rot': [(Y, -4)]}, 'arm_R': {'rot': [(Y, 4)]},
        'tail_1': {'rot': [(X, 8)]}, 'tail_3': {'rot': [(X, 10), (Z, 6 * p)]},
    }


ACTIONS = [('idle', 48, idle, 4), ('wave', 48, wave, 3), ('celebrate', 36, celebrate, 2),
           ('sleep', 96, sleep, 8), ('sad', 48, sad, 2)]

rig.animation_data_create()
made = []
for name, length, fn, step in ACTIONS:
    act = bpy.data.actions.new(name)
    act.use_fake_user = True
    rig.animation_data.action = act
    frames = sorted(set(list(range(0, length + 1, step)) + [length]))
    for f in frames:
        spec = fn(f % length)
        pose(f + 1, spec)
    act.frame_range = (1, length + 1)
    try:
        act.use_cyclic = True
    except Exception:
        pass
    made.append(act)
rig.animation_data.action = made[0]
scene.frame_start, scene.frame_end = 1, 48

# ---------------------------------------------------------------- save and export
bpy.ops.wm.save_as_mainfile(filepath=os.path.join(HERE, 'sprig-mascot.blend'))
bpy.ops.export_scene.gltf(filepath=os.path.join(HERE, 'sprig-mascot.glb'), export_format='GLB',
                          export_apply=True, export_animations=True, export_animation_mode='ACTIONS',
                          export_yup=True)
print('EXPORTED', [a.name for a in made])

if not RENDER:
    sys.exit(0)

# ---------------------------------------------------------------- previews


def toonify(m, hexcol, flat=False):
    """Two-tone cel shading for the preview renders: lit colour and one shade band."""
    nt = m.node_tree
    nt.nodes.clear()
    out_n = nt.nodes.new('ShaderNodeOutputMaterial')
    emit = nt.nodes.new('ShaderNodeEmission')
    nt.links.new(emit.outputs['Emission'], out_n.inputs['Surface'])
    if flat:
        emit.inputs['Color'].default_value = lin(hexcol)
        return
    diff = nt.nodes.new('ShaderNodeBsdfDiffuse')
    s2r = nt.nodes.new('ShaderNodeShaderToRGB')
    ramp = nt.nodes.new('ShaderNodeValToRGB')
    ramp.color_ramp.interpolation = 'CONSTANT'
    ramp.color_ramp.elements[0].position = 0.0
    ramp.color_ramp.elements[0].color = (0.62, 0.66, 0.74, 1)
    ramp.color_ramp.elements[1].position = 0.22
    ramp.color_ramp.elements[1].color = (1, 1, 1, 1)
    mix = nt.nodes.new('ShaderNodeMix')
    mix.data_type, mix.blend_type = 'RGBA', 'MULTIPLY'
    mix.inputs['Factor'].default_value = 1.0
    rgba = [i for i in mix.inputs if i.type == 'RGBA']
    rgba[0].default_value = lin(hexcol)
    nt.links.new(diff.outputs['BSDF'], s2r.inputs['Shader'])
    nt.links.new(s2r.outputs['Color'], ramp.inputs['Fac'])
    nt.links.new(ramp.outputs['Color'], rgba[1])
    res = [o for o in mix.outputs if o.type == 'RGBA'][0]
    nt.links.new(res, emit.inputs['Color'])


for k, m in MAT.items():
    toonify(m, PALETTE.get(k, PALETTE['body']), flat=k in ('outline', 'eye', 'white', 'mouth'))

# skin: colour from the painted attribute, plus a fine bumpy texture for the renders
nt = MAT['skin'].node_tree
mix = next(n for n in nt.nodes if n.bl_idname == 'ShaderNodeMix')
attr = nt.nodes.new('ShaderNodeVertexColor')
attr.layer_name = 'skin'
nt.links.new(attr.outputs['Color'], [i for i in mix.inputs if i.type == 'RGBA'][0])
tex = nt.nodes.new('ShaderNodeTexNoise')
tex.inputs['Scale'].default_value = 90.0
tex.inputs['Detail'].default_value = 3.0
bump = nt.nodes.new('ShaderNodeBump')
bump.inputs['Strength'].default_value = 0.35
bump.inputs['Distance'].default_value = 0.02
nt.links.new(tex.outputs['Fac'], bump.inputs['Height'])
diff = next(n for n in nt.nodes if n.bl_idname == 'ShaderNodeBsdfDiffuse')
nt.links.new(bump.outputs['Normal'], diff.inputs['Normal'])
out = os.path.join(HERE, 'previews')
os.makedirs(out, exist_ok=True)
for engine in ('BLENDER_EEVEE', 'BLENDER_EEVEE_NEXT'):
    try:
        scene.render.engine = engine
        break
    except TypeError:
        continue
try:
    scene.eevee.taa_render_samples = 32
except Exception:
    pass
scene.view_settings.view_transform = 'Standard'
scene.render.resolution_x = scene.render.resolution_y = 480
scene.render.film_transparent = False
world = bpy.data.worlds.new('world')
scene.world = world
try:
    world.use_nodes = True
except Exception:
    pass
bg = world.node_tree.nodes['Background']
bg.inputs['Color'].default_value = lin('#12313A')
bg.inputs['Strength'].default_value = 1.0

key = bpy.data.objects.new('key', bpy.data.lights.new('key', 'SUN'))
key.data.energy = 3.2
key.data.use_shadow = False
key.rotation_euler = (math.radians(50), math.radians(-10), math.radians(-35))
coll.objects.link(key)
fill = bpy.data.objects.new('fill', bpy.data.lights.new('fill', 'SUN'))
fill.data.energy = 0.0
fill.rotation_euler = (math.radians(70), 0, math.radians(150))
coll.objects.link(fill)
amb = bpy.data.objects.new('amb', bpy.data.lights.new('amb', 'SUN'))
amb.data.energy = 0.0
amb.rotation_euler = (math.radians(-60), 0, 0)
coll.objects.link(amb)

target = bpy.data.objects.new('target', None)
target.location = (0, 0.2, 1.08)
coll.objects.link(target)
cam = bpy.data.objects.new('cam', bpy.data.cameras.new('cam'))
cam.data.type = 'ORTHO'
cam.data.ortho_scale = 3.25
coll.objects.link(cam)
scene.camera = cam
c = cam.constraints.new('TRACK_TO')
c.target = target
c.track_axis, c.up_axis = 'TRACK_NEGATIVE_Z', 'UP_Y'


def place_camera(yaw_deg, elev_deg=12, dist=8):
    a, e = math.radians(yaw_deg), math.radians(elev_deg)
    cam.location = (target.location.x + dist * math.sin(a) * math.cos(e),
                    target.location.y - dist * math.cos(a) * math.cos(e),
                    target.location.z + dist * math.sin(e))


# turnaround in the idle rest pose
rig.animation_data.action = made[0]
scene.frame_set(1)
for label, yaw in (('front', 0), ('three-quarter', 35), ('side', 90), ('back', 180)):
    place_camera(yaw)
    scene.render.filepath = os.path.join(out, f'turnaround-{label}.png')
    bpy.ops.render.render(write_still=True)

if '--stills' in ARGS:
    sys.exit(0)

# one frame sequence per action (GIFs are assembled outside Blender)
place_camera(-14)
scene.render.resolution_x = scene.render.resolution_y = 360
seq = os.path.join(out, 'frames')
for act, (name, length, _fn, _step) in zip(made, ACTIONS):
    rig.animation_data.action = act
    for f in range(1, length + 1, 2):
        scene.frame_set(f)
        scene.render.filepath = os.path.join(seq, name, f'{f:03d}.png')
        bpy.ops.render.render(write_still=True)
print('RENDERED')
