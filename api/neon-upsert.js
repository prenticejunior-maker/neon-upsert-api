import { neon } from "@neondatabase/serverless";

const sql = neon(process.env.DATABASE_URL);

function quoteIdent(name) {
  return `"${String(name).replace(/"/g, '""')}"`;
}

function quoteTable(name) {
  const parts = String(name).split(".");
  if (parts.length === 1) return quoteIdent(parts[0]);
  if (parts.length === 2) return `${quoteIdent(parts[0])}.${quoteIdent(parts[1])}`;
  throw new Error("Nome de tabela inválido");
}

function chunkArray(arr, size) {
  const out = [];
  for (let i = 0; i < arr.length; i += size) {
    out.push(arr.slice(i, i + size));
  }
  return out;
}

export default async function handler(req, res) {

  try {

    if (req.method !== "POST") {
      return res.status(405).json({ erro: "Method not allowed" });
    }

    const auth = req.headers.authorization || "";

    if (auth !== `Bearer ${process.env.API_TOKEN}`) {
      return res.status(401).json({ erro: "Unauthorized" });
    }

    const body = typeof req.body === "string"
      ? JSON.parse(req.body)
      : req.body;

    const tabelaDestino = body.tabela_destino;
    const colunasChave = body.colunas_chave;
    const linhas = body.linhas;

    if (!linhas || !linhas.length) {
      return res.json({ ok: true, linhas_processadas: 0 });
    }

    const colunas = Object.keys(linhas[0]);

    const tabelaSql = quoteTable(tabelaDestino);

    const colsSql = colunas.map(quoteIdent).join(",");

    const conflitoSql = colunasChave.map(quoteIdent).join(",");

    const updateCols = colunas.filter(c => !colunasChave.includes(c));

    const updateSql = updateCols
      .map(c => `${quoteIdent(c)} = EXCLUDED.${quoteIdent(c)}`)
      .join(",");

    let total = 0;

    const lotes = chunkArray(linhas, 200);

    for (const lote of lotes) {

      const values = [];
      const rowsSql = [];

      lote.forEach((linha, rowIndex) => {

        const placeholders = [];

        colunas.forEach((col, colIndex) => {

          values.push(linha[col] ?? null);

          placeholders.push(`$${rowIndex * colunas.length + colIndex + 1}`);

        });

        rowsSql.push(`(${placeholders.join(",")})`);

      });

      const query = `
        INSERT INTO ${tabelaSql} (${colsSql})
        VALUES ${rowsSql.join(",")}
        ON CONFLICT (${conflitoSql})
        DO UPDATE SET ${updateSql}
      `;

      await sql.query(query, values);

      total += lote.length;

    }

    return res.json({
      ok: true,
      linhas_processadas: total
    });

  } catch (e) {

    return res.status(500).json({
      erro: e.message,
      stack: e.stack
    });

  }

}
