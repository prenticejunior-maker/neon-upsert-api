import { Pool } from "pg";

const pool = new Pool({
  connectionString: process.env.NEON_DATABASE_URL,
  ssl: {
    rejectUnauthorized: false,
  },
});

export default async function handler(req, res) {
  if (req.method !== "POST") {
    return res.status(405).json({
      ok: false,
      erro: "Método não permitido. Use POST.",
    });
  }

  try {
    const authHeader = req.headers.authorization || "";
    const tokenRecebido = authHeader.replace("Bearer ", "").trim();
    const tokenEsperado = process.env.API_TOKEN;

    if (!tokenRecebido || tokenRecebido !== tokenEsperado) {
      return res.status(401).json({
        ok: false,
        erro: "Token inválido.",
      });
    }

    const client = await pool.connect();

    try {
      const inicio = new Date();

      await client.query("CALL teste_desenvolvimento.prc_etl_receita_rede();");

      const fim = new Date();

      return res.status(200).json({
        ok: true,
        mensagem: "Procedure executada com sucesso.",
        procedure: "teste_desenvolvimento.prc_etl_receita_rede",
        inicio: inicio.toISOString(),
        fim: fim.toISOString(),
      });
    } finally {
      client.release();
    }
  } catch (error) {
    return res.status(500).json({
      ok: false,
      erro: error.message || String(error),
    });
  }
}
