import { neon } from "@neondatabase/serverless";

const sql = neon(process.env.DATABASE_URL);

export default async function handler(req, res) {
  try {
    if (req.method !== "POST") {
      return res.status(405).json({
        ok: false,
        erro: "Método não permitido. Use POST."
      });
    }

    const auth = req.headers.authorization || "";
    if (auth !== `Bearer ${process.env.API_TOKEN}`) {
      return res.status(401).json({
        ok: false,
        erro: "Unauthorized"
      });
    }

    const inicio = new Date();

    await sql`call teste_desenvolvimento.prc_etl_receita_rede();`;

    const fim = new Date();

    return res.status(200).json({
      ok: true,
      mensagem: "Procedure executada com sucesso.",
      procedure: "teste_desenvolvimento.prc_etl_receita_rede",
      inicio: inicio.toISOString(),
      fim: fim.toISOString()
    });

  } catch (e) {
    return res.status(500).json({
      ok: false,
      erro: e.message,
      stack: e.stack
    });
  }
}
