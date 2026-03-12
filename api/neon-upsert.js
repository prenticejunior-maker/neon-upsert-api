import { neon } from "@neondatabase/serverless";

const sql = neon(process.env.DATABASE_URL);

function quoteIdent(name) {
  return `"${String(name).replace(/"/g, '""')}"`;
}

function quoteTable(name) {
  const parts = String(name).split(".");
  if (parts.length === 1) return { schema: "public", table: parts[0] };
  if (parts.length === 2) return { schema: parts[0], table: parts[1] };
  throw new Error("Nome de tabela inválido");
}

function chunkArray(arr, size) {
  const out = [];
  for (let i = 0; i < arr.length; i += size) out.push(arr.slice(i, i + size));
  return out;
}

function normalizeColName(name) {
  return String(name || "")
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "_")
    .replace(/^_+|_+$/g, "")
    .replace(/_+/g, "_");
}

function toDateOnly(value) {
  if (value == null || value === "") return null;

  const txt = String(value).trim();

  if (/^\d{4}-\d{2}-\d{2}$/.test(txt)) return txt;

  const br = txt.match(/^(\d{2})\/(\d{2})\/(\d{4})$/);
  if (br) return `${br[3]}-${br[2]}-${br[1]}`;

  const isoDateTime = txt.match(/^(\d{4}-\d{2}-\d{2})[ T].*$/);
  if (isoDateTime) return isoDateTime[1];

  return null;
}

function toTimestamp(value) {
  if (value == null || value === "") return null;

  const txt = String(value).trim();

  if (/^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$/.test(txt)) return txt;

  const iso = txt.match(/^(\d{4}-\d{2}-\d{2})T(\d{2}:\d{2}:\d{2})/);
  if (iso) return `${iso[1]} ${iso[2]}`;

  const br = txt.match(/^(\d{2})\/(\d{2})\/(\d{4})[ ](\d{2}:\d{2}:\d{2})$/);
  if (br) return `${br[3]}-${br[2]}-${br[1]} ${br[4]}`;

  return null;
}

function sanitizeValue(col, value) {
  if (col === "data") return toDateOnly(value);
  if (col === "data_processamento") return toTimestamp(value);
  if (value === "") return null;
  return value;
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

    const body = typeof req.body === "string" ? JSON.parse(req.body) : req.body;

    const tabelaDestino = body.tabela_destino;
    const colunasChaveOriginais = body.colunas_chave || [];
    const linhasOriginais = body.linhas || [];

    if (!tabelaDestino) {
      return res.status(400).json({ erro: "tabela_destino não informada" });
    }

    if (!linhasOriginais.length) {
      return res.json({ ok: true, linhas_processadas: 0 });
    }

    const { schema, table } = quoteTable(tabelaDestino);

    const colsResult = await sql.query(
      `
      SELECT column_name
      FROM information_schema.columns
      WHERE table_schema = $1
        AND table_name = $2
      ORDER BY ordinal_position
      `,
      [schema, table]
    );

    const colunasTabela = colsResult.map(function (r) {
      return r.column_name;
    });

    if (!colunasTabela.length) {
      throw new Error(`Tabela não encontrada ou sem colunas: ${tabelaDestino}`);
    }

    const mapaTabela = {};
    colunasTabela.forEach(col => {
      mapaTabela[normalizeColName(col)] = col;
    });

    const colunasEntrada = Object.keys(linhasOriginais[0]);
    const mapeamentoEntradaParaTabela = {};

    colunasEntrada.forEach(colEntrada => {
      const normalizada = normalizeColName(colEntrada);
      if (mapaTabela[normalizada]) {
        mapeamentoEntradaParaTabela[colEntrada] = mapaTabela[normalizada];
      }
    });

    const colunasDestino = Object.values(mapeamentoEntradaParaTabela);

    if (!colunasDestino.length) {
      throw new Error("Nenhuma coluna da planilha corresponde às colunas da tabela no Neon.");
    }

    const colunasChave = colunasChaveOriginais
      .map(c => mapaTabela[normalizeColName(c)])
      .filter(Boolean);

    if (!colunasChave.length) {
      throw new Error("Nenhuma coluna-chave corresponde às colunas reais da tabela no Neon.");
    }

    const linhas = linhasOriginais.map(linha => {
      const novaLinha = {};
      Object.keys(mapeamentoEntradaParaTabela).forEach(colEntrada => {
        const colTabela = mapeamentoEntradaParaTabela[colEntrada];
        novaLinha[colTabela] = sanitizeValue(colTabela, linha[colEntrada]);
      });
      return novaLinha;
    });

    const colsSql = colunasDestino.map(quoteIdent).join(",");
    const conflitoSql = colunasChave.map(quoteIdent).join(",");

    const updateCols = colunasDestino.filter(c => !colunasChave.includes(c));
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

        colunasDestino.forEach((col, colIndex) => {
          values.push(linha[col] ?? null);
          placeholders.push(`$${rowIndex * colunasDestino.length + colIndex + 1}`);
        });

        rowsSql.push(`(${placeholders.join(",")})`);
      });

      const query = `
        INSERT INTO ${quoteIdent(schema)}.${quoteIdent(table)} (${colsSql})
        VALUES ${rowsSql.join(",")}
        ON CONFLICT (${conflitoSql})
        DO UPDATE SET ${updateSql}
      `;

      await sql.query(query, values);
      total += lote.length;
    }

    return res.json({
      ok: true,
      linhas_processadas: total,
      colunas_recebidas: colunasEntrada.length,
      colunas_utilizadas: colunasDestino.length,
      colunas_chave_utilizadas: colunasChave
    });

  } catch (e) {
    return res.status(500).json({
      erro: e.message,
      stack: e.stack
    });
  }
}
