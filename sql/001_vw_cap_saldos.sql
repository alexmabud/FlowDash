-- View: vw_cap_saldos
-- Cálculo padronizado de competência, principal coberto e saldo em aberto no CAP.

CREATE VIEW IF NOT EXISTS vw_cap_saldos AS
SELECT
  cam.id,
  cam.credor,
  cam.status,
  cam.parcela_num,
  cam.parcela_de,
  cam.data_vencimento,

  -- Valor da competência (face value da parcela)
  COALESCE(cam.valor_evento, cam.valor_a_pagar, cam.valor)                 AS valor_competencia,

  -- Acumulados que saem do banco/caixa
  COALESCE(cam.valor_pago_acumulado, 0)                                    AS pago,
  COALESCE(cam.juros_pago_acumulado, 0)                                    AS juros,
  COALESCE(cam.multa_paga_acumulada, 0)                                    AS multa,
  COALESCE(cam.desconto_aplicado_acumulado, 0)                             AS desconto,

  -- Principal já coberto (o que abate a competência)
  (COALESCE(cam.valor_pago_acumulado,0)
   - COALESCE(cam.juros_pago_acumulado,0)
   - COALESCE(cam.multa_paga_acumulada,0)
   + COALESCE(cam.desconto_aplicado_acumulado,0))                          AS principal_coberto_acum,

  -- Saldo ainda em aberto (usaremos no dropdown)
  (COALESCE(cam.valor_evento, cam.valor_a_pagar, cam.valor)
   - (
      COALESCE(cam.valor_pago_acumulado,0)
      - COALESCE(cam.juros_pago_acumulado,0)
      - COALESCE(cam.multa_paga_acumulada,0)
      + COALESCE(cam.desconto_aplicado_acumulado,0)
     )
  ) AS saldo_em_aberto

FROM contas_a_pagar_mov cam;
