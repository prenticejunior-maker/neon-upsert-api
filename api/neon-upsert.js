import { neon } from "@neondatabase/serverless";

const sql = neon(process.env.DATABASE_URL);

function quoteIdent(name) {
  return `"${String(name).replace(/"/g, '""')}"`;
}

function quoteTable(fullName) {
  const parts = String(fullName || "").split(".");
  if (parts.length === 1) return quoteIdent(parts[0]);
  if (parts.length === 2) return `${quoteIdent(parts[0])}.${quoteIdent(parts[1])}`;
  throw new Error(`Nome de tabela inválido: ${fullName}`);
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

    const body = typeof req.body === "string" ? JSON.parse(req.body) : (req.body || {});
    const tabelaDestino = body.tabela_destino;
    const colunasChave = Array.isArray(body.colunas_chave) ? body.colunas_chave : [];
    const linhas = Array.isArray(body.linhas) ? body.linhas : [];

    if (!tabelaDestino) {
      return res.status(400).json({ erro: "tabela_destino não informada" });
    }

    if (!colunasChave.length) {
      return res.status(400).json({ erro: "colunas_chave não informada" });
    }

    if (!linhas.length) {
      return res.status(200).json({ ok: true, mensagem: "sem linhas", linhas_processadas: 0 });
    }

    const colunas = Object.keys(linhas[0]);
    const tabelaSql = quoteTable(tabelaDestino);
    const colsSql = colunas.map(quoteIdent).join(", ");
    const conflitoSql = colunasChave.map(quoteIdent).join(", ");

    const updateCols = colunas.filter((c) => !colunasChave.includes(c));
    if (!updateCols.length) {
      return res.status(400).json({ erro: "Não há colunas para update no ON CONFLICT" });
    }

    const updateSql = updateCols
      .map((c) => `${quoteIdent(c)} = EXCLUDED.${quoteIdent(c)}`)
      .join(", ");

    let totalProcessado = 0;
    const lotes = chunkArray(linhas, 200);

    for (const lote of lotes) {
      const values = [];
      const rowsSql = [];

      lote.forEach((linha, rowIdx) => {
        const placeholders = [];

        colunas.forEach((col, colIdx) => {
          values.push(linha[col] ?? null);
          placeholders.push(`$${rowIdx * colunas.length + colIdx + 1}`);
        });

        rowsSql.push(`(${placeholders.join(", ")})`);
      });

      const query = `
        INSERT INTO ${tabelaSql} (${colsSql})
        VALUES ${rowsSql.join(", ")}
        ON CONFLICT (${conflitoSql})
        DO UPDATE SET ${updateSql}
      `;

      await sql(query, values);
      totalProcessado += lote.length;
    }

    return res.status(200).json({
      ok: true,
      tabela_destino: tabelaDestino,
      linhas_recebidas: linhas.length,
      linhas_processadas: totalProcessado
    });
  } catch (e) {
    return res.status(500).json({
      ok: false,
      erro: e && e.message ? e.message : String(e),
      stack: e && e.stack ? String(e.stack).split("\n").slice(0, 5) : null
    });
  }
}
