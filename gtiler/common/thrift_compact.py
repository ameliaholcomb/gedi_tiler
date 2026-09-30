"""A lossless reader and writer for the Thrift compact protocol, enough to
edit a parquet footer (FileMetaData) without its IDL.

A struct is a list of [field_id, type, value]. Values are ints, bytes,
nested structs, or lists [element_type, [values]]. Encoding what decode()
returned gives back the same bytes, so fields this module does not know
about (e.g. new logical types) survive an edit.
"""

STOP, TRUE, FALSE, BYTE, I16, I32, I64, DOUBLE, BINARY, LIST, SET, MAP, STRUCT = range(13)


def _read_varint(buf, pos):
    shift = result = 0
    while True:
        b = buf[pos]
        pos += 1
        result |= (b & 0x7F) << shift
        if not b & 0x80:
            return result, pos
        shift += 7


def _write_varint(n):
    out = bytearray()
    while True:
        b = n & 0x7F
        n >>= 7
        if n:
            out.append(b | 0x80)
        else:
            out.append(b)
            return bytes(out)


def _unzigzag(n):
    return (n >> 1) ^ -(n & 1)


def _zigzag(n):
    return (n << 1) ^ (n >> 63)


def _read_value(buf, pos, t):
    if t in (TRUE, FALSE):  # a bool list element: one byte, 1 for true
        return buf[pos] == 1, pos + 1
    if t == BYTE:
        return buf[pos], pos + 1
    if t in (I16, I32, I64):
        n, pos = _read_varint(buf, pos)
        return _unzigzag(n), pos
    if t == DOUBLE:
        return bytes(buf[pos : pos + 8]), pos + 8
    if t == BINARY:
        n, pos = _read_varint(buf, pos)
        return bytes(buf[pos : pos + n]), pos + n
    if t in (LIST, SET):
        head = buf[pos]
        pos += 1
        size, etype = head >> 4, head & 0x0F
        if size == 15:
            size, pos = _read_varint(buf, pos)
        items = []
        for _ in range(size):
            v, pos = _read_value(buf, pos, etype)
            items.append(v)
        return [etype, items], pos
    if t == STRUCT:
        return _read_struct(buf, pos)
    raise ValueError(f"unsupported compact type {t}")


def _read_struct(buf, pos):
    fields, last = [], 0
    while True:
        head = buf[pos]
        pos += 1
        t = head & 0x0F
        if t == STOP:
            return fields, pos
        delta = head >> 4
        if delta:
            fid = last + delta
        else:
            n, pos = _read_varint(buf, pos)
            fid = _unzigzag(n)
        if t in (TRUE, FALSE):
            value = t == TRUE
        else:
            value, pos = _read_value(buf, pos, t)
        fields.append([fid, t, value])
        last = fid


def _write_value(t, v):
    if t in (TRUE, FALSE):
        return bytes([1 if v else 2])
    if t == BYTE:
        return bytes([v])
    if t in (I16, I32, I64):
        return _write_varint(_zigzag(v))
    if t == DOUBLE:
        return v
    if t == BINARY:
        return _write_varint(len(v)) + v
    if t in (LIST, SET):
        etype, items = v
        head = (
            bytes([(len(items) << 4) | etype])
            if len(items) < 15
            else bytes([0xF0 | etype]) + _write_varint(len(items))
        )
        return head + b"".join(_write_value(etype, x) for x in items)
    if t == STRUCT:
        return encode(v)
    raise ValueError(f"unsupported compact type {t}")


def encode(fields):
    out, last = bytearray(), 0
    for fid, t, v in fields:
        wire_t = (TRUE if v else FALSE) if t in (TRUE, FALSE) else t
        delta = fid - last
        if 0 < delta <= 15:
            out.append((delta << 4) | wire_t)
        else:
            out.append(wire_t)
            out += _write_varint(_zigzag(fid))
        if t not in (TRUE, FALSE):
            out += _write_value(t, v)
        last = fid
    out.append(STOP)
    return bytes(out)


def decode(buf):
    """Decode one struct, returning it and the bytes it took."""
    fields, end = _read_struct(memoryview(buf), 0)
    return fields, end


def field(fields, fid):
    """The value of a field, or None if absent."""
    for f, _, v in fields:
        if f == fid:
            return v
    return None


def set_field(fields, fid, t, value):
    """Set a field, keeping the fields in id order."""
    for f in fields:
        if f[0] == fid:
            f[1], f[2] = t, value
            return
    fields.append([fid, t, value])
    fields.sort(key=lambda f: f[0])
