using Microsoft.VisualBasic;
using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace baixaEuro
{
    class BaixaBoleto
    {
        string number;
        // ============================= buscar e baixar pagamentos
        public void baixaAlt(string sqlSource, string strcon, string conta, string sLabel, ref string ID_AX)
        {
           
            string buscarAX;
            buscarAX = "";
            buscarAX += " SELECT * FROM PFAXMOVIMENTO ";
            buscarAX += " WHERE 1=1 ";
            buscarAX += "       AND PFAXOCORRENCIA IN (6) ";
            buscarAX += "       AND FKAXID = " + ID_AX;

            SqlConnection conexao = new SqlConnection(strcon);
            SqlCommand cmd;

            try
            {
                conexao.Open();
                cmd = new SqlCommand(buscarAX, conexao);
                cmd.ExecuteNonQuery(); // executa cmd
                SqlDataReader buscaAxMov = cmd.ExecuteReader();
                if (buscaAxMov.HasRows)
                {
                    DadosBoletoBaixa baixa;
                    EnvioNfe notaFiscal;
                    while (buscaAxMov.Read())
                    {
                        number = buscaAxMov["PFAXMOVSEUNUM"].ToString();
                        // tipobol = 0 / Graduacao
                        // tipobol = 1 / Extensão
                        // tipobol = 2

                        baixa = null;
                        baixa = new DadosBoletoBaixa();
                        baixa.sStringConexao = strcon;
                        baixa.sSqlSource = sqlSource;
                        baixa.carregaClasse(buscaAxMov);
                        baixa.sAgeCon = conta;
                        baixa.identIes(sLabel);
                        

                        if (baixa.bAchouX9 && !baixa.bPreInscricao)
                        {
                            if (baixa.sTipoOcorrencia == "6" && !baixa.bVestibular)
                            {

                                if (baixa.sSeqReg != null && baixa.sAnoReg != null)
                                {
                                    baixa.bPagDuplicado = true;
                                    if (baixa.bEuro)
                                    {
                                        notaFiscal = null;
                                        notaFiscal = new EnvioNfe();
                                        notaFiscal.gravaDadosNota(ref baixa);
                                    }
                                }
                                else
                                {

                                    baixa.preparaSqlInsert76();
                                    ConnInsert classCon = new ConnInsert();
                                    SqlConnection con = classCon.abrirConexao(baixa.sStringConexao);

                                    SqlCommand cmd1 = con.CreateCommand();
                                    SqlTransaction tran76 = con.BeginTransaction();
                                    try
                                    {
                                        cmd1.CommandText = baixa.sSqlInsere76;
                                        cmd1.Transaction = tran76;
                                        cmd1.ExecuteNonQuery();
                                        baixa.buscaW6(ref con, ref tran76);
                                        baixa.binseriu76 = true;
                                        tran76.Commit();
                                    }
                                    catch (Exception ex)
                                    {
                                        //SE TIVER ALGUM ERRO, ENTAO...
                                        tran76.Rollback();
                                        Console.WriteLine("Erro ao Baixar Arquivo PF76 : " + ID_AX + "Nosso Numero: " + baixa.sNossoNumero + " Erro: " + ex);
                                    }
                                    finally
                                    {
                                        
                                        con.Close();
                                        classCon.fecharConexao();
                                        classCon = null;
                                        tran76 = null;
                                        baixa.busca76();
                                        if (baixa.bEuro)
                                        {
                                           notaFiscal = null;
                                           notaFiscal = new EnvioNfe();
                                           notaFiscal.gravaDadosNota(ref baixa);
                                        }
                                    }
                                }
                                if (baixa.binseriu76 && !baixa.bPagDuplicado)
                                {
                                    if (baixa.bBiblioteca)
                                    {
                                        //ConnInsert classCon = new ConnInsert();
                                        //string conBiblioteca = "Data Source=CL2DB;Initial Catalog=BDSCBP;User ID=User_Ceuma;Password=#nvC&#m@(2O19.4)?*";
                                        //SqlConnection con = classCon.abrirConexao(conBiblioteca);
                                        //SqlTransaction tranB = con.BeginTransaction();

                                        //string sSql = "";
                                        //sSql = sSql + " UPDATE USU_USUARIOS SET USU_SALDO = (USU_SALDO + " + baixa.dValorPago + " ) " +
                                        //              " WHERE USU_CODIGO_NOVO = '" + baixa.sCpdAluno + "'";

                                        //SqlCommand cmdb = con.CreateCommand();
                                        //try
                                        //{
                                        //    cmdb.CommandText = sSql;
                                        //    cmdb.Transaction = tranB;
                                        //    cmdb.ExecuteNonQuery(); // EXECUTA CMD
                                        //    tranB.Commit();
                                        //    con.Close();
                                        //    classCon.fecharConexao();
                                        //    classCon = null;
                                        //    tranB = null;
                                        //}
                                        //catch
                                        //{
                                        //    tranB.Rollback();
                                        //    con.Close();
                                        //    classCon.fecharConexao();
                                        //    tranB = null;
                                        //    classCon = null;
                                        //    Console.WriteLine("Erro ao Baixar Pagamento de Biblioteca ID: " + ID_AX + " Nosso Numero: " + baixa.sNossoNumero);

                                        //}
                                    }

                                    else
                                    {
                                        ConnInsert classCon = new ConnInsert();
                                        SqlConnection con = classCon.abrirConexao(baixa.sStringConexao);
                                        SqlTransaction tranBaixa = con.BeginTransaction();

                                        //INICIA BAIXA DE BOLETOS
                                        if (baixa.bGraduacao)
                                        {
                                            InsereGraduacao insereGraduacao = new InsereGraduacao();
                                            insereGraduacao.inserir(ref baixa, ref tranBaixa, ref con);
                                        }
                                        else if (baixa.bExtensao && !baixa.bPreInscricao)
                                        {
                                            InsereExtensao insereExtensao = new InsereExtensao();
                                            insereExtensao.inserir(ref baixa, ref tranBaixa, ref con);
                                        }

                                        //else if (baixa.bVestibular)
                                        //{
                                        //    // baixa.busca82(baixa.sServico_vest, ref con, ref tranBaixa);
                                        //    // baixa.baixaVestibular(ref con, ref tranBaixa);
                                        //}
                                        //else if (baixa.bAchouVestibular)
                                        //{
                                        //   // baixa.alocarAluno(ref con, ref tranBaixa);
                                        //}
                                        //else if (baixa.bBiblioteca)
                                        //{
                                        //    string sSql = "";
                                        //    sSql = sSql + " UPDATE USU_USUARIOS SET USU_SALDO = '0'" +
                                        //                  " WHERE USU_CODIGO = '" + baixa.sCpdAluno + "'";

                                        //    SqlCommand cmd1 = con.CreateCommand();
                                        //    try
                                        //    {
                                        //        cmd1.CommandText = sSql;
                                        //        cmd1.Transaction = tranBaixa;
                                        //        cmd1.ExecuteNonQuery(); // EXECUTA CMD
                                        //    }
                                        //    catch
                                        //    {
                                        //        baixa.bRollback = true;
                                        //    }
                                        //}

                                        //SE TIVER ALGUM ERRO, ENTAO...
                                        if (baixa.bRollback)
                                        {
                                            tranBaixa.Rollback();
                                            con.Close();
                                            classCon.fecharConexao();
                                            classCon = null;
                                            Console.WriteLine("Erro ao Quitar Parcela no Sistema: " + ID_AX + " Nosso Numero: " + baixa.sNossoNumero);
                                        }
                                        else
                                        {
                                            tranBaixa.Commit();
                                            con.Close();
                                            classCon.fecharConexao();
                                            classCon = null;
                                            tranBaixa = null;
                                        }
                                    }
                                }
                                else
                                {
                                    baixa.bPagDuplicado = true;
                                }
                            }
                            else if (baixa.sTipoOcorrencia == "2" && !baixa.bVestibular)
                            {
                                ConnInsert classCon = new ConnInsert();
                                SqlConnection con = classCon.abrirConexao(baixa.sStringConexao);
                                SqlTransaction tranBaixa = con.BeginTransaction();

                                baixa.buscaW6Ref2(ref con, ref tranBaixa);

                                if (baixa.bRollback)
                                {
                                    tranBaixa.Rollback();
                                    con.Close();
                                    classCon.fecharConexao();
                                    tranBaixa = null;
                                    classCon = null;
                                    Console.WriteLine("Erro ao Registrar Boleto na W9 no Sistema: " + ID_AX + " Nosso Numero: " + baixa.sNossoNumero);
                                }
                                else
                                {
                                    tranBaixa.Commit();
                                    con.Close();
                                    classCon.fecharConexao();
                                    classCon = null;
                                    tranBaixa = null;
                                }
                            }
                        }
                        //else if (baixa.bExtensao && baixa.bPreInscricao) TESTE NTI APENAS PAGAMENTOS SELECIONADOS...
                        else if (false)// NÃO ENTRAR NESSE METODO AGORA...
                        {
                            if (baixa.sTipoOcorrencia == "6")
                            {
                                baixa.identTipoPreInscricao();
                                baixa.baixaPreInscricao();
                            }
                            else
                            {
                                ConnInsert classCon = new ConnInsert();
                                SqlConnection con = classCon.abrirConexao(baixa.sStringConexao);
                                SqlTransaction tranBaixa = con.BeginTransaction();

                                baixa.buscaW6Ref2(ref con, ref tranBaixa);

                                if (baixa.bRollback)
                                {
                                    tranBaixa.Rollback();
                                    con.Close();
                                    classCon.fecharConexao();
                                    classCon = null;
                                    Console.WriteLine("Erro ao Registrar Boleto na W9 no Sistema: " + ID_AX + " Nosso Numero: " + baixa.sNossoNumero);
                                }
                                else
                                {
                                    tranBaixa.Commit();
                                    con.Close();
                                    classCon.fecharConexao();
                                    classCon = null;
                                    tranBaixa = null;
                                }
                            }
                        }//DESCOMENTAR DEPOIS DE TODOS OS TESTES...
                        else
                        {
                            Console.WriteLine("Baixa não encontrada na X9 com esse ID : " + ID_AX + "Nosso Numero: " + baixa.sNossoNumero);
                        }
                        if (baixa.bCartaCredito && !baixa.bPagDuplicado)
                        {
                            ConnInsert classCon = new ConnInsert();
                            SqlConnection con = classCon.abrirConexao(baixa.sStringConexao);
                            SqlTransaction tranCarta = con.BeginTransaction();

                            baixa.inserirCartaCredito(ref tranCarta, ref con);

                            if (baixa.bRollback)
                            {
                                tranCarta.Rollback();
                                con.Close();
                                classCon.fecharConexao();
                                classCon = null;
                                Console.WriteLine("Erro ao Inserir Carta de Credito no Sistema: " + ID_AX + " Nosso Numero: " + baixa.sNossoNumero);
                            }
                            else
                            {
                                tranCarta.Commit();
                                con.Close();
                                classCon.fecharConexao();
                                classCon = null;
                                tranCarta = null;
                            }
                        }
                    }//FIM do LAÇO
                }
                else
                {
                    Console.WriteLine("Não foram encontrados Baixas com esse ID : " + ID_AX);
                }

            }
            catch
            {
                Console.WriteLine("Baixa não foi totalmente concluida, existe pendencias..."+ number);
            }
            finally
            {
                //registraBaixa(ID_AX, strcon);
                Console.WriteLine("Baixa Finalizada ID : " + ID_AX + " || Data: " + DateTime.Now);
                conexao.Close();
                conexao = null;
            }
        }
        public void registraBaixa(string id, string sConexao)
        {
            string sql;
            sql = " UPDATE PFAXHEADER "
               + "       SET PFAXDATBAIXA = GETDATE() , "
               + "       PFAXUSUBAIXA = 'REAL'"
               + "  WHERE  PFAXID = " + id;

            ConnInsert classCon = new ConnInsert();
            SqlConnection con = classCon.abrirConexao(sConexao);
            SqlCommand cmd1 = con.CreateCommand();
            SqlTransaction tran = con.BeginTransaction();

            cmd1.CommandText = sql;
            cmd1.Transaction = tran;

            try
            {
                cmd1.ExecuteNonQuery(); // EXECUTA CMD
            }
            catch
            {
                tran.Rollback();
                con.Close();
                classCon.fecharConexao();
                classCon = null;
                tran = null;

            }
            finally
            {
                tran.Commit();
                con.Close();
                classCon.fecharConexao();
            }
        }

    }
}

