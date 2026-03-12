import { neon } from "@neondatabase/serverless";

const sql = neon(process.env.DATABASE_URL);

export default async function handler(req, res) {

  if (req.method !== "POST") {
    return res.status(405).json({ erro: "Method not allowed" });
  }

  const token = req.headers.authorization;

  if (token !== `Bearer ${process.env.API_TOKEN}`) {
    return res.status(401).json({ erro: "Unauthorized" });
  }

  const { tabela_destino, colunas_chave, linhas } = req.body;

  if (!linhas || !linhas.length) {
    return res.json({ ok: true, mensagem: "sem linhas" });
  }

  const colunas = Object.keys(linhas[0]);

  const cols = colunas.map(c => `"${c}"`).join(",");

  const updates = colunas
    .filter(c => !colunas_chave.includes(c))
    .map(c => `"${c}" = EXCLUDED."${c}"`)
    .join(",");

  const conflito = colunas_chave.map(c => `"${c}"`).join(",");

  for (const linha of linhas) {

    const valores = colunas.map(c => linha[c]);

    const placeholders = valores.map((_,i)=>`$${i+1}`).join(",");

    const query = `
      INSERT INTO ${tabela_destino} (${cols})
      VALUES (${placeholders})
      ON CONFLICT (${conflito})
      DO UPDATE SET ${updates}
    `;

    await sql.query(query, valores);
  }

  return res.json({
    ok:true,
    linhas_processadas:linhas.length
  });

}
