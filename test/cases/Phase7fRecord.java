// =============================================================================
// Phase7fRecord — custom Hadoop Writable record class for the PXF
// hdfs:SequenceFile profile (Phase 7f external-tables acceptance).
//
// PXF's WritableResolver (org.apache.cloudberry.pxf.plugins.hdfs.WritableResolver)
// requires a user-supplied Java class, passed via the DATA-SCHEMA option in the
// pxf:// LOCATION URI, that implements org.apache.hadoop.io.Writable. PXF maps the
// PUBLIC fields of this class, in declaration order, to the external-table columns.
//
// This record matches the writable/readable external tables:
//   (id int, name text, amount bigint, price float8)
//
// The same class is used for BOTH the writable (pxfwritable_export) and the
// readable (pxfwritable_import) external table so the SequenceFile round-trips.
//
// Compiled with `--release 11` (the PXF sidecar runs OpenJDK 11 JRE) against the
// Hadoop 2.10.2 jars bundled in the PXF application jar, and the resulting
// Phase7fRecord.class is dropped into /pxf-base/lib (on the PXF classpath) on
// every segment-primary PXF sidecar.
// =============================================================================
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class Phase7fRecord implements Writable {
    // Public fields, in column order. PXF's WritableResolver reflects over these.
    public int    id;
    public String name;
    public long   amount;
    public double price;

    public Phase7fRecord() {
        this.name = "";
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(id);
        byte[] nameBytes = (name == null ? "" : name).getBytes("UTF-8");
        out.writeInt(nameBytes.length);
        out.write(nameBytes);
        out.writeLong(amount);
        out.writeDouble(price);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        id = in.readInt();
        int nameLen = in.readInt();
        byte[] nameBytes = new byte[nameLen];
        in.readFully(nameBytes);
        name = new String(nameBytes, "UTF-8");
        amount = in.readLong();
        price = in.readDouble();
    }
}
