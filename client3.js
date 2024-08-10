const net = require("net");
const fs = require("fs").promises;

const HOST = "localhost";
const PORT = 3000;

const PACKET_CONTENTS = [
  { name: "symbol", type: "ascii", size: 4 },
  { name: "buysellindicator", type: "ascii", size: 1 },
  { name: "quantity", type: "int32", size: 4 },
  { name: "price", type: "int32", size: 4 },
  { name: "packetSequence", type: "int32", size: 4 },
];

const PACKET_SIZE = PACKET_CONTENTS.reduce(
  (total, item) => total + item.size,
  0
);

function parsePacket(buffer) {
  if (buffer.length !== PACKET_SIZE) {
    throw new Error(`Invalid packet size: ${buffer.length}`);
  }

  let offset = 0;
  const packet = {};

  PACKET_CONTENTS.forEach((field) => {
    const { name, type, size } = field;
    if (type === "ascii") {
      packet[name] = buffer.toString("ascii", offset, offset + size).trim();
    } else if (type === "int32") {
      packet[name] = buffer.readInt32BE(offset);
    }
    offset += size;
  });

  return packet;
}

function validatePacket(packet) {
  if (typeof packet !== "object") return false;
  if (packet.symbol.length !== 4) return false;
  if (!["B", "S"].includes(packet.buysellindicator)) return false;
  if (!Number.isInteger(packet.quantity) || packet.quantity <= 0) return false;
  if (!Number.isInteger(packet.price) || packet.price <= 0) return false;
  if (!Number.isInteger(packet.packetSequence) || packet.packetSequence <= 0)
    return false;
  return true;
}

function createRequestPayload(callType, resendSeq = 0) {
  const buffer = Buffer.alloc(2);
  buffer.writeUInt8(callType, 0);
  buffer.writeUInt8(resendSeq, 1);
  return buffer;
}

function requestAllPackets() {
  return new Promise((resolve, reject) => {
    const client = new net.Socket();
    const packets = [];

    client.connect(PORT, HOST, () => {
      console.log("Connected to server");
      client.write(createRequestPayload(1));
    });

    client.on("data", (data) => {
      for (let i = 0; i < data.length; i += PACKET_SIZE) {
        const packetBuffer = data.slice(i, i + PACKET_SIZE);
        try {
          const packet = parsePacket(packetBuffer);
          if (validatePacket(packet)) {
            packets.push(packet);
          } else {
            console.error("Invalid packet:", packet);
          }
        } catch (error) {
          console.error("Error parsing packet:", error.message);
        }
      }
    });

    client.on("close", () => {
      console.log("Connection closed");
      resolve(packets);
    });

    client.on("error", (err) => {
      reject(err);
    });
  });
}

async function requestMissingPacket(seq, retries = 3) {
  for (let attempt = 1; attempt <= retries; attempt++) {
    try {
      return await new Promise((resolve, reject) => {
        const client = new net.Socket();

        client.connect(PORT, HOST, () => {
          console.log(
            `Requesting missing packet ${seq} (Attempt ${attempt}/${retries})`
          );
          client.write(createRequestPayload(2, seq));
        });

        client.on("data", (data) => {
          const packet = parsePacket(data);
          client.destroy();
          resolve(packet);
        });

        client.on("error", (err) => {
          client.destroy();
          reject(err);
        });

        // Add a timeout to prevent hanging
        setTimeout(() => {
          client.destroy();
          reject(new Error(`Timeout requesting packet ${seq}`));
        }, 5000);
      });
    } catch (error) {
      console.error(
        `Failed to get packet ${seq} (Attempt ${attempt}/${retries}):`,
        error.message
      );
      if (attempt === retries) {
        throw error; // Rethrow the error if all retries have been exhausted
      }
    }
  }
}

async function requestMissingPackets(missingSequences) {
  const missingPackets = [];

  for (const seq of missingSequences) {
    try {
      const packet = await requestMissingPacket(seq);
      missingPackets.push(packet);
    } catch (error) {
      console.error(`Failed to get packet ${seq} after all retries`);
    }
  }

  return missingPackets;
}

async function main() {
  try {
    console.log("Requesting all packets...");
    const allPackets = await requestAllPackets();

    const sequences = allPackets.map((p) => p.packetSequence);
    const maxSequence = Math.max(...sequences);
    const missingSequences = Array.from(
      { length: maxSequence },
      (_, i) => i + 1
    ).filter((seq) => !sequences.includes(seq));

    console.log("Missing sequences:", missingSequences);

    if (missingSequences.length > 0) {
      console.log("Requesting missing packets...");
      const missingPackets = await requestMissingPackets(missingSequences);
      allPackets.push(...missingPackets);
    }

    const sortedPackets = allPackets.sort(
      (a, b) => a.packetSequence - b.packetSequence
    );

    // Verify complete sequence
    const finalSequences = sortedPackets.map((p) => p.packetSequence);
    const expectedSequences = Array.from(
      { length: maxSequence },
      (_, i) => i + 1
    );
    if (!expectedSequences.every((seq) => finalSequences.includes(seq))) {
      console.error("Warning: Not all sequences are present in the final data");
    }

    const outputFileName = "stock_data.json";
    await fs.writeFile(outputFileName, JSON.stringify(sortedPackets, null, 2));
    console.log(`Data saved to ${outputFileName}`);
  } catch (error) {
    console.error("An error occurred:", error);
  }
}

main();
