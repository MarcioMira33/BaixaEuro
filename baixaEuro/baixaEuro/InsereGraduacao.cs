using System;
using System.Data;
using System.Data.SqlClient;

namespace baixaEuro
{
    class InsereGraduacao
    {
        //METODO INICIAL ------------------------------------------------------------------------------------ Anderson Passos
        public void inserir(ref DadosBoletoBaixa baixa, ref SqlTransaction tran, ref SqlConnection con)
        {
            if (baixa.bMensalidade)
            {
                baixaMensalidade(ref baixa, ref tran, ref con);
            }
            else if (baixa.bNegociacao)
            {
                baixaNegociacao(ref baixa, ref tran, ref con);
            }
            else if (baixa.bServico)
            {
                baixaServico(ref baixa, ref tran, ref con);
            }
        }

        public void baixaMensalidade(ref DadosBoletoBaixa baixa, ref SqlTransaction tran, ref SqlConnection con)
        {

            //baixa.busca47();
            //if (baixa.bAchou47)
            //{ //VERIFICA SE JÁ ESTÁ PAGA!....                                
            //    if (baixa.sSitBol == "2") // 2 EM ABERTO --- 2,3,8,p,a,f, s, t
            //    {
            //        if (baixa.sCpdAluno != "" && baixa.sAnoSemestre != "" && baixa.sCodBol != "" && baixa.sSeqReg != "")
            //        {
            //            //ATUALIZAÇÃO NA PF48
            //            string sQuery = "";
            //            sQuery = sQuery + "UPDATE   PF48EFIMET ";
            //            sQuery = sQuery + "SET      PF48VALPAG = REPLACE('" + baixa.dValorPago + "', ',' ,'.') , ";
            //            sQuery = sQuery + "         PF48DATPAG = '" + baixa.sDataPagamento + "'";
            //            sQuery = sQuery + "WHERE    FK4847CPDALU = " + baixa.sCpdAluno;
            //            sQuery = sQuery + "         AND FK4847ANOBOL = " + baixa.sAnoSemestre;
            //            sQuery = sQuery + "         AND FK4847CODBOL =  " + baixa.sCodBol;

            //            // INSERE NA BASE... 
            //            SqlCommand cmd1 = con.CreateCommand();
            //            try
            //            {
            //                cmd1.CommandText = sQuery;
            //                cmd1.Transaction = tran;
            //                cmd1.ExecuteNonQuery(); // EXECUTA CMD
            //            }
            //            catch
            //            {
            //                baixa.bRollback = true;
            //                return;
            //            }
            //            string CamposValores = "";

            //            CamposValores = CamposValores + " PF47DATPAG = '" + baixa.sDataPagamento + "'";
            //            CamposValores = CamposValores + ", PF47VALMUL = REPLACE('" + baixa.dMulMes + "', ',' ,'.') ";
            //            CamposValores = CamposValores + ", PF47SITBOL = 1 ";
            //            CamposValores = CamposValores + ", PF76ANOREG = " + baixa.sAnoReg;
            //            CamposValores = CamposValores + ", PF76SEQREG = " + baixa.sSeqReg;
            //            CamposValores = CamposValores + ", PF47VALPAG = REPLACE('" + baixa.dValorPago + "', ',' ,'.') ";
            //            CamposValores = CamposValores + ", PF47OBSQUI =  '" + baixa.sMsgQ + "'";

            //            string sSql = "";
            //            sSql = sSql + " UPDATE PF47BOLETT ";
            //            sSql = sSql + " SET " + CamposValores;
            //            sSql = sSql + "   WHERE  FK4737CPDALU = " + baixa.sCpdAluno;
            //            sSql = sSql + "       And PF47CODBOL = " + baixa.sCodBol;
            //            sSql = sSql + "       And PF47ANOBOL = " + baixa.sAnoSemestre;

            //            // INSERE NA BASE... 
            //            SqlCommand cmd2 = con.CreateCommand();
            //            try
            //            {
            //                cmd2.CommandText = sSql;
            //                cmd2.Transaction = tran;
            //                cmd2.ExecuteNonQuery();
            //            }
            //            catch
            //            {
            //                baixa.bRollback = true;
            //                return;
            //            }
            //        }
            //    }
            //}

            baixa.busca47();
            if (baixa.bAchou47)
            {
                foreach (DataRow row in baixa.dtretorno47.Rows)
                {
                    if (baixa.baixa47(ref tran, ref con, row))
                    {
                        baixa.baixa48(ref tran, ref con, row);
                    }
                }
            }
        }

        public void baixaNegociacao(ref DadosBoletoBaixa baixa, ref SqlTransaction tran, ref SqlConnection con)
        {
            baixa.busca46();
            if (baixa.bAchou46)
            {
                if (baixa.baixa46(ref tran, ref con)) // BAIXA  NEGOCIADO
                {
                    baixa.busca47();
                    if (baixa.bAchou47)
                    {
                        foreach (DataRow row in baixa.dtretorno47.Rows)
                        {
                            if (baixa.baixa47(ref tran, ref con, row))
                            {
                                baixa.baixa48(ref tran, ref con, row);
                            }
                        }
                    }
                    else
                    {
                        if (baixa.recuperaNegociacao())
                        {
                            foreach (DataRow row in baixa.dtretorno47.Rows)
                            {
                                if (baixa.baixa47(ref tran, ref con, row))
                                {
                                    baixa.baixa48(ref tran, ref con, row);
                                }
                            }
                        }
                    }
                }
            }
            else
            {
                Console.WriteLine("Boleto com Status de Cancelado: Baixa ID: " + baixa.iIdMov + "Nosso Numero: " + baixa.sNossoNumero);
                baixa.bCartaCredito = true;
            }

        }

        public void baixaServico(ref DadosBoletoBaixa baixa, ref SqlTransaction tran, ref SqlConnection con)
        {
            //VERIFICA E BAIXA D6
            baixa.baixaD6(ref tran, ref con);

            if (baixa.bBaixouD6)//verificar se tem registros do serviço na D6
            {
                if (baixa.sCodProtocolo != null && baixa.sCodProtocolo != "")
                {
                    baixa.baixa52(ref tran, ref con);//baixa na PF52

                }
                else if (baixa.sCodServico == "1258")
                {
                    ConnInsert classCon = new ConnInsert();
                    string conBiblioteca = "Data Source=CL3DB;Initial Catalog=BDSCBP;User ID=User_GrpCeuma;Password=G*pC&#m@08.19?";
                    SqlConnection conb = classCon.abrirConexao(conBiblioteca);

                    string sSql = "";
                    sSql = sSql + " UPDATE USU_USUARIOS SET USU_SALDO = (USU_SALDO + REPLACE('" + baixa.dValorPago + "', ',' ,'.') ) " +
                                  " WHERE USU_CODIGO_NOVO = '" + baixa.sCpdAluno + "'";

                    SqlCommand cmdb = conb.CreateCommand();
                    try
                    {
                        cmdb.CommandText = sSql;
                        cmdb.ExecuteNonQuery(); // EXECUTA CMD
                        conb.Close();
                        classCon.fecharConexao();
                        classCon = null;

                    }
                    catch
                    {
                        conb.Close();
                        classCon.fecharConexao();
                        Console.WriteLine("Erro ao Baixar Pagamento de Biblioteca ID: Nosso Numero: " + baixa.sNossoNumero);

                    }
                }
                else
                {
                    baixa.bCartaCredito = true;
                }
            }

        }
    }
}
