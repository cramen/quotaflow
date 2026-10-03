package io.quotaflow.tck;

import jdk.jfr.consumer.RecordedFrame;
import org.springframework.asm.*;

/** Build-only bytecode proof for cold VM resolution events lacking a ClassLoader Java frame. */
final class InitializationInstruction {
    private record Code(ClassReader reader, int offset, int length, int access, boolean monitor) { }
    static boolean provesResolution(RecordedFrame frame) {
        try {
            if (!"Interpreted".equals(frame.getType())) return false;
            var method = frame.getMethod();
            String owner = method.getType().getName().replace('.', '/');
            var code = read(owner, method.getName(), method.getDescriptor());
            if (code == null || code.monitor || (code.access & Opcodes.ACC_SYNCHRONIZED) != 0) return false;
            int bci = frame.getBytecodeIndex();
            if (bci < 0 || bci + 2 >= code.length) return false;
            var reader = code.reader; int offset = code.offset + bci;
            int opcode = reader.readByte(offset); int item = reader.getItem(reader.readUnsignedShort(offset + 1));
            char[] buffer = new char[reader.getMaxStringLength()];
            if (opcode == Opcodes.NEW) {
                String target = reader.readUTF8(item, buffer);
                return owner.startsWith("io/quotaflow/core/") && target.startsWith("io/quotaflow/core/");
            }
            if (opcode != Opcodes.INVOKESTATIC && opcode != Opcodes.GETSTATIC && opcode != Opcodes.PUTSTATIC) return false;
            String target = reader.readUTF8(reader.getItem(reader.readUnsignedShort(item)), buffer);
            if (!target.startsWith("io/quotaflow/core/") && !target.startsWith("io/quotaflow/store/")
                    && !target.startsWith("io/quotaflow/fallback/")) return false;
            if (opcode == Opcodes.GETSTATIC || opcode == Opcodes.PUTSTATIC) return true;
            int nameType = reader.getItem(reader.readUnsignedShort(item + 2));
            String name = reader.readUTF8(nameType, buffer), descriptor = reader.readUTF8(nameType + 2, buffer);
            var called = read(target, name, descriptor);
            // The interpreted caller has not entered the Java method. Reject native,
            // synchronized or monitor-containing targets: their entry is not sufficient
            // evidence of VM-only resolution. No generic class-name exemption is used.
            return called != null && !called.monitor && (called.access & (Opcodes.ACC_SYNCHRONIZED | Opcodes.ACC_NATIVE)) == 0;
        } catch (Exception unavailable) { return false; }
    }
    private static Code read(String owner, String name, String descriptor) throws Exception {
        try (var stream = InitializationInstruction.class.getClassLoader().getResourceAsStream(owner + ".class")) {
            if (stream == null) return null;
            var reader = new ClassReader(stream); var monitor = new boolean[1];
            reader.accept(new ClassVisitor(Opcodes.ASM9) {
                @Override public MethodVisitor visitMethod(int access, String method, String signature, String generic, String[] exceptions) {
                    if (!method.equals(name) || !signature.equals(descriptor)) return null;
                    return new MethodVisitor(Opcodes.ASM9) {
                        @Override public void visitInsn(int opcode) { if (opcode == Opcodes.MONITORENTER) monitor[0] = true; }
                    };
                }
            }, ClassReader.SKIP_DEBUG | ClassReader.SKIP_FRAMES);
            char[] buffer = new char[reader.getMaxStringLength()];
            int cursor = reader.header + 8 + 2 * reader.readUnsignedShort(reader.header + 6);
            int fields = reader.readUnsignedShort(cursor); cursor += 2;
            for (int field = 0; field < fields; field++) cursor = skipMember(reader, cursor);
            int methods = reader.readUnsignedShort(cursor); cursor += 2;
            for (int method = 0; method < methods; method++) {
                boolean matches = name.equals(reader.readUTF8(cursor + 2, buffer)) && descriptor.equals(reader.readUTF8(cursor + 4, buffer));
                int access = reader.readUnsignedShort(cursor), attributes = reader.readUnsignedShort(cursor + 6); cursor += 8;
                for (int attribute = 0; attribute < attributes; attribute++) {
                    if (matches && "Code".equals(reader.readUTF8(cursor, buffer)))
                        return new Code(reader, cursor + 14, reader.readInt(cursor + 10), access, monitor[0]);
                    cursor += 6 + reader.readInt(cursor + 2);
                }
            }
            return null;
        }
    }
    private static int skipMember(ClassReader reader, int offset) {
        int attributes = reader.readUnsignedShort(offset + 6); offset += 8;
        for (int i = 0; i < attributes; i++) offset += 6 + reader.readInt(offset + 2);
        return offset;
    }
}
