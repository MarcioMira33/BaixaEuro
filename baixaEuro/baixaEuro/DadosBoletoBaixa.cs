using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using Microsoft.VisualBasic;
using System.Security.Cryptography;
using System.Data;


namespace baixaEuro
{
    public class DadosBoletoBaixa
    {//CLASS para pegar dados do boleto para usar na baixa
        string buscar;

        Connection cConn = new Connection();

        string baixar;
        string sSql;
        string codProdutoNfe;

        //vestibular
        private string anoVestibular;
        private string codVestibular;
        private string diaVestibular;
        private string codHorario;
        private string servico_vest = "223";
        public string sServico_vest { get { return servico_vest; } }

        //VARIAVEIS DA PFAXMOVIMENTO
        private string seuNumero;
        private string nossoNumero;
        private string dataVencimento;
        private string dataPagamento;
        private Decimal valorPago;
        private Decimal valorRecebido;
        private string tipoOcorrencia;
        private string seqArq;
        private bool rollback = false;

        //VARIAVEIS DA PFX9
        private string ano;
        private string anoSemestre;
        private string codBol;
        private string cpdAluno;
        private string numInscricao;
        private string valorBase;
        private string codIes;
        private string cnpjNfe;

        //VARIAVEIS DA PFX9
        private string anoReg;
        private string seqReg;

        //VAR DE CONN
        private string dataSource;
        private string stringConexao;
        private string tab;
        private int idMov;
        private const string msgQ = "Quitado-OK CISA";

        //VAR PF47
        private string sitBol;

        // bool
        private bool achouX9 = false;
        private bool nfeinternetcaixa = false;
        private bool achou76 = false;
        private bool baixouD6 = false;
        private bool baixou52 = false;
        private bool inseriu76 = false;
        private bool achou47 = false;
        private bool achou46 = false;
        private bool cartaCredito = false;
        private bool achouVestibular = false;
        private bool alocaAluno = false;
        private bool graduacao = false;
        private bool extensao = false;
        private bool vestibular = false;
        private bool biblioteca = false;
        private bool pagDuplicado = false;
        private bool mensalidade = false;
        private bool negociacao = false;
        private bool servico = false;
        private bool preInscricao = false;
        private bool turmaExtensao = false;
        private bool matExtensao = false;
        private bool preMatExtensao = false;

        //PARA INSERT
        private string buscarCred;
        private string sqlCartCred;
        private string sqlCampos;
        private string sqlValores;
        private string sqlInsere76;
        private string tipoBol;
        private string formPag;
        private string tipoServ;

        private string locPag;
        private string bolEmo;
        private string desPag;
        private double mulMes;
        private string desBan;
        private string numVia;
        private string usuCai;
        private string codCam;
        private string ageCon;
        private string locTra;
        private string codProtocolo;

        //VARIAVEIS DA BAIXA PRE-INSCRIÇÃO
        private string turno;
        private string turma;
        private string campus;
        private string codTurno;
        private decimal percentualPagamento;
        private string curso;
        private string codCurso;
        private string valorTotalCurso;
        private string inscricoes;
        private string msgPre;
        private string assunto;
        private string destino;
        private string email;

        private string matriculaAluno;
        private string plaFinPos;
        private string codigoAluno;
        private string nomeAluno;
        private string enderecoAluno;
        private string bairroAluno;
        private string cidadeAluno;
        private string unifed;
        private string cepAluno;
        private string emailAluno;
        private string tel1Aluno;
        private string tel2Aluno;
        private string celAluno;
        private string identAluno;
        private string orgErgAluno;
        private string cpfAluno;
        private string dataNascAluno;
        private string naturalAluno;
        private string sexoAluno;
        private string cpdMatAluno;
        private string indFunAluno;
        private string indProAluno;
        private string crGradAluno;
        private string locGraAluno;
        private string numInscricaoAluno;
        private string situacaoAluno;
        private string codTurma;
        private string dataPagTurma;
        private string catalog;
        private string sqlSource;
        private string codServico;
        private string notasBaixadas;
        private DataTable retorno47;
        private bool recuperadoNeg;

        public bool bRecuperadoNeg { get { return recuperadoNeg; } set { recuperadoNeg = value; } }
        public string sNotasBaixadas { get { return notasBaixadas; } set { notasBaixadas = value; } }
        public DataTable dtretorno47 { get { return retorno47; } set { retorno47 = value; } }
        public string sCodServico { get { return codServico; } set { codServico = value; } }

        //GET SET DA PRE-INCRIÇÃO
        public string sCnpjNfe { get { return cnpjNfe; } set { cnpjNfe = value; } }
        public string sCampus { get { return campus; } set { campus = value; } }
        public string sDataPagTurma { get { return dataPagTurma; } set { dataPagTurma = value; } }
        public string sCodTurno { get { return codTurno; } set { codTurno = value; } }
        public string sTurno { get { return turno; } set { turno = value; } }
        public string sTurma { get { return turma; } set { turma = value; } }
        public decimal sPercentualPagamento { get { return percentualPagamento; } set { percentualPagamento = value; } }
        public string sCodCurso { get { return codCurso; } set { codCurso = value; } }
        public string sCursoAluno { get { return curso; } set { curso = value; } }
        public string sValorTotalCurso { get { return valorTotalCurso; } set { valorTotalCurso = value; } }
        public string sInscricoes { get { return inscricoes; } set { inscricoes = value; } }
        public string sMsgPre { get { return msgPre; } set { msgPre = value; } }
        public string sAssunto { get { return assunto; } set { assunto = value; } }
        public string sDestino { get { return destino; } set { destino = value; } }
        public string sEmail { get { return email; } set { email = value; } }

        public string sMatriculaAluno { get { return matriculaAluno; } set { matriculaAluno = value; } }
        public string sPlanoFinPos { get { return plaFinPos; } set { plaFinPos = value; } }
        public string sCodigoCurso { get { return codigoAluno; } set { codigoAluno = value; } }
        public string sNomeAluno { get { return nomeAluno; } set { nomeAluno = value; } }
        public string sEnderecoAluno { get { return enderecoAluno; } set { enderecoAluno = value; } }
        public string sBairroAluno { get { return bairroAluno; } set { bairroAluno = value; } }
        public string sCidadeAluno { get { return cidadeAluno; } set { cidadeAluno = value; } }
        public string sUnifed { get { return unifed; } set { unifed = value; } }
        public string sCepAluno { get { return cepAluno; } set { cepAluno = value; } }
        public string sEmailAluno { get { return emailAluno; } set { emailAluno = value; } }
        public string sTel1Aluno { get { return tel1Aluno; } set { tel1Aluno = value; } }
        public string sTel2Aluno { get { return tel2Aluno; } set { tel2Aluno = value; } }
        public string sCelAluno { get { return celAluno; } set { celAluno = value; } }
        public string sIdentAluno { get { return identAluno; } set { identAluno = value; } }
        public string sOrgErgAluno { get { return orgErgAluno; } set { orgErgAluno = value; } }
        public string sCpfAluno { get { return cpfAluno; } set { cpfAluno = value; } }
        public string sDataNascAluno { get { return dataNascAluno; } set { dataNascAluno = value; } }
        public string sNaturalAluno { get { return naturalAluno; } set { naturalAluno = value; } }
        public string sSexoAluno { get { return sexoAluno; } set { sexoAluno = value; } }

        public string sCpdMatAluno { get { return cpdMatAluno; } set { cpdMatAluno = value; } }
        public string sIndFunAluno { get { return indFunAluno; } set { indFunAluno = value; } }
        public string sIndProAluno { get { return indProAluno; } set { indProAluno = value; } }
        public string sCrGradAluno { get { return crGradAluno; } set { crGradAluno = value; } }
        public string sLocGraAluno { get { return locGraAluno; } set { locGraAluno = value; } }

        public string sNumInscricaoAluno { get { return numInscricaoAluno; } set { numInscricaoAluno = value; } }
        public string sSituacaoAluno { get { return situacaoAluno; } set { situacaoAluno = value; } }
        public string sCodTurma { get { return codTurma; } set { codTurma = value; } }
        public string sCodProdutoNfe { get { return codProdutoNfe; } set { codProdutoNfe = value; } }

        //bool
        private bool achou57 = false;
        private bool inseriu20 = false;

        //bool
        public bool sAchou57 { get { return achou57; } set { achou57 = value; } }
        public bool bNfeinternetcaixa { get { return nfeinternetcaixa; } set { nfeinternetcaixa = value; } }
        public bool bInseriu20 { get { return inseriu20; } set { inseriu20 = value; } }

        //strings

        //GET SET DA PFAXMOVIMENTO

        public string sSqlSource { get { return sqlSource; } set { sqlSource = value; } }
        public string sSeuNumero { get { return seuNumero; } set { seuNumero = value; } }
        public string sNossoNumero { get { return nossoNumero; } set { nossoNumero = value; } }
        public string sDataVencimento { get { return dataVencimento; } set { dataVencimento = value; } }
        public string sDataPagamento { get { return dataPagamento; } set { dataPagamento = value; } }
        public Decimal dValorPago { get { return valorPago; } set { valorPago = value; } }
        public Decimal dValorRecebido { get { return valorRecebido; } set { valorRecebido = value; } }
        public string sTipoOcorrencia { get { return tipoOcorrencia; } set { tipoOcorrencia = value; } }
        public string sSeqArq { get { return seqArq; } set { seqArq = value; } }

        //GET SET DA PFX9
        public string sAno { get { return ano; } set { ano = value; } }
        public string sAnoSemestre { get { return anoSemestre; } set { anoSemestre = value; } }
        public string sCodBol { get { return codBol; } set { codBol = value; } }
        public string sCpdAluno { get { return cpdAluno; } set { cpdAluno = value; } }
        public string sNumInscricao { get { return numInscricao; } set { numInscricao = value; } }
        public string sValorBase { get { return valorBase; } set { valorBase = value; } }
        public string sCodIes { get { return codIes; } set { codIes = value; } }

        //GET SET DA pf76
        public string sAnoReg { get { return anoReg; } set { anoReg = value; } }
        public string sSeqReg { get { return seqReg; } set { seqReg = value; } }

        //GET SET DA pf47
        public string sSitBol { get { return sitBol; } set { sitBol = value; } }

        public string sBuscarCred { get { return buscarCred; } set { buscarCred = value; } }
        public string sDataSource { get { return dataSource; } set { dataSource = value; } }
        public string sStringConexao { get { return stringConexao; } set { stringConexao = value; } }
        public int iIdMov { get { return idMov; } set { idMov = value; } }
        public bool bAchouX9 { get { return achouX9; } set { achouX9 = value; } }
        public bool bRollback { get { return rollback; } set { rollback = value; } }
        public bool bAchou76 { get { return achou76; } set { achou76 = value; } }
        public bool binseriu76 { get { return inseriu76; } set { inseriu76 = value; } }
        public bool bAchou47 { get { return achou47; } set { achou47 = value; } }
        public bool bAchou46 { get { return achou46; } set { achou46 = value; } }
        public bool bCartaCredito { get { return cartaCredito; } set { cartaCredito = value; } }
        public bool bAchouVestibular { get { return achouVestibular; } set { achouVestibular = value; } }
        public bool bAlocaAluno { get { return alocaAluno; } set { alocaAluno = value; } }
        public bool bGraduacao { get { return graduacao; } set { graduacao = value; } }
        public bool bExtensao { get { return extensao; } set { extensao = value; } }
        public bool bVestibular { get { return vestibular; } set { vestibular = value; } }
        public bool bBiblioteca { get { return biblioteca; } set { biblioteca = value; } }
        public bool bMensalidade { get { return mensalidade; } set { mensalidade = value; } }
        public bool bNegociacao { get { return negociacao; } set { negociacao = value; } }
        public bool bServico { get { return servico; } set { servico = value; } }
        public bool bPreInscricao { get { return preInscricao; } set { preInscricao = value; } }
        public bool bTurmaExtensao { get { return turmaExtensao; } set { turmaExtensao = value; } }
        public bool bMatExtensao { get { return matExtensao; } set { matExtensao = value; } }
        public bool bPreMatExtensao { get { return preMatExtensao; } set { preMatExtensao = value; } }

        public bool bPagDuplicado { get { return pagDuplicado; } set { pagDuplicado = value; } }
        public bool bBaixouD6 { get { return baixouD6; } set { baixouD6 = value; } }
        public bool bBaixou52 { get { return baixou52; } set { baixou52 = value; } }

        public string sMsgQ { get { return msgQ; } }
        public string sTab { get { return tab; } set { tab = value; } }
        public string sCatalog { get { return catalog; } set { catalog = value; } }
        public string sSqlCampos { get { return sqlCampos; } set { sqlCampos = value; } }
        public string sSqlValores { get { return sqlValores; } set { sqlValores = value; } }
        public string sSqlInsere76 { get { return sqlInsere76; } set { sqlInsere76 = value; } }
        public string sTipoBol { get { return tipoBol; } set { tipoBol = value; } }
        public string sFormPag { get { return formPag; } set { formPag = value; } }
        public string sTipoServ { get { return tipoServ; } set { tipoServ = value; } }
        public string sSqlCartCred { get { return sqlCartCred; } set { sqlCartCred = value; } }

        public string sLocPag { get { return locPag; } set { locPag = value; } }
        public string sBolEmo { get { return bolEmo; } set { bolEmo = value; } }
        public string sDesPag { get { return desPag; } set { desPag = value; } }
        public double dMulMes { get { return mulMes; } set { mulMes = value; } }
        public string sDesBan { get { return desBan; } set { desBan = value; } }
        public string sNumVia { get { return numVia; } set { numVia = value; } }
        public string sUsuCai { get { return usuCai; } set { usuCai = value; } }
        public string sCodCam { get { return codCam; } set { codCam = value; } }
        public string sAgeCon { get { return ageCon; } set { ageCon = value; } }
        public string sLocTra { get { return locTra; } set { locTra = value; } }
        public string sCodProtocolo { get { return codProtocolo; } set { codProtocolo = value; } }

        //GET SET vestibular
        public string sAnoVestibular { get { return anoVestibular; } set { anoVestibular = value; } }
        public string sCodVestibular { get { return codVestibular; } set { codVestibular = value; } }
        public string sDiaVestibular { get { return diaVestibular; } set { diaVestibular = value; } }
        public string sCodHorario { get { return codHorario; } set { codHorario = value; } }


        private bool famaz = false;
        private bool euro = false;
        private bool cesup = false;
        private bool ceupi = false;
        private bool ceuma = false;

        public bool bFamaz { get { return famaz; } set { famaz = value; } }
        public bool bEuro { get { return euro; } set { euro = value; } }
        public bool bCesup { get { return cesup; } set { cesup = value; } }
        public bool bCeupi { get { return ceupi; } set { ceupi = value; } }
        public bool bCeuma { get { return ceuma; } set { ceuma = value; } }

        //FIM DECLARAÇÃO DE VARIAVEL E GET/SET
        public void preparaStringCon()
        {
            string conn = "Data Source=" + sSqlSource + ";Initial Catalog=" + sCatalog + ";User ID=User_Ceuma;Password=#nvC&#m@(2O19.4)?*";
            sStringConexao = conn;
        }
        public void limparDados()
        {
            //ainda a programar

        }
        //PREPARA SQL
        public void preparaSqlInsert76()
        {
            carregaCamposPF76();
            carregaValoresPF76();
            sSqlInsere76 = "INSERT INTO " + sTab + "76MOVFIT ( " + sSqlCampos + " ) " +
                                        " VALUES ( " + sSqlValores + " ) ";
        }
        //CAMPOS DE INSERIR NA 76
        public void carregaCamposPF76()
        {
            if (bGraduacao)
            {
                sqlCampos = "";
                sqlCampos = sqlCampos + "PF76ANOREG, PF76SEQREG, FK7637CPDALU, PF76VALPAG, PF76DATEVE, ";
                sqlCampos = sqlCampos + "PF76LOCPAG, PF76BOLEMO, PF76DESPAG, PF76VALMES, PF76MULMES, PF76DESBAN, ";
                sqlCampos = sqlCampos + "PF76NUMVIA, PF76USUCAI, PF76CODCAM, PF76CODBOL, PF76ANOBOL, PF76SEQARQ, PF76AGECON,";
                sqlCampos = sqlCampos + "PF76LOCTRA, FK76X9BANCONOSSONUM, FK76X9SEUNUMERO";

            }
            else if (bExtensao)
            {
                sqlCampos = "";
                sqlCampos = sqlCampos + "EX76ANOREG, EX76SEQREG, FK7637CPDALU, EX76VALPAG, EX76DATEVE, ";
                sqlCampos = sqlCampos + "EX76LOCPAG, EX76BOLEMO, EX76DESPAG, EX76VALMES, EX76MULMES, EX76DESBAN, ";
                sqlCampos = sqlCampos + "EX76NUMVIA, EX76USUCAI, EX76CODCAM, EX76CODBOL, EX76ANOBOL, EX76SEQARQ, EX76AGECON, ";
                sqlCampos = sqlCampos + "EX76LOCTRA, FK76X9BANCONOSSONUM, FK76X9SEUNUMERO";

            }
            else if (bVestibular)
            {
                sqlCampos = "";
            }
        }
        // VALORES PARA INSERIR NA 76

        public void carregaValoresPF76()
        {
            string anoRegSql = " (SELECT " + sTab + "76ANOREG = DATENAME(YEAR, GetDate())) ";
            string seqRegSql = " (SELECT ISNULL(MAX(" + sTab + "76SEQREG), 0)+1 FROM " + sTab + "76MOVFIT" +
                          " WHERE " + sTab + "76ANOREG = DATENAME(YEAR, GetDate())) ";
            sSqlValores = "";
            sSqlValores = sSqlValores + anoRegSql + " , ";
            sSqlValores = sSqlValores + seqRegSql + " , ";
            sSqlValores = sSqlValores + "'" + sCpdAluno + "' , ";
            sSqlValores = sSqlValores + "REPLACE('" + dValorPago + "', ',' ,'.') , ";
            sSqlValores = sSqlValores + "'" + sDataPagamento + "' , ";
            sSqlValores = sSqlValores + "'" + sLocPag + "' , ";
            sSqlValores = sSqlValores + "'" + sBolEmo + "' , ";
            sSqlValores = sSqlValores + "'" + sDesPag + "' , ";
            sSqlValores = sSqlValores + "REPLACE('" + sValorBase + "', ',' ,'.') , ";
            sSqlValores = sSqlValores + "REPLACE('" + dMulMes + "', ',' ,'.') , ";
            sSqlValores = sSqlValores + "'" + sDesBan + "' , ";
            sSqlValores = sSqlValores + "'" + sNumVia + "' , ";
            sSqlValores = sSqlValores + "'" + sUsuCai + "' , ";
            sSqlValores = sSqlValores + "'" + sCodCam + "' , ";
            sSqlValores = sSqlValores + "'" + sCodBol + "' , ";
            sSqlValores = sSqlValores + "'" + sAnoSemestre + "' , ";
            sSqlValores = sSqlValores + "'" + sSeqArq + "' , ";
            sSqlValores = sSqlValores + "'" + sAgeCon + "' , ";
            sSqlValores = sSqlValores + "'" + sLocTra + "' , ";
            sSqlValores = sSqlValores + "'" + sNossoNumero + "' , ";
            sSqlValores = sSqlValores + "'" + sSeuNumero + "'";
        }

        public void preparaSqlCartaCred()
        {
            string msqCarta = "Registro Carta de Credito";

            sSqlCartCred = sSqlCartCred + "INSERT INTO " + sTab + "E3CONCOT ( FKE337CPDALU, " + sTab + "E3DATCRE, " + sTab + "E3TIPCRE , ";
            sSqlCartCred = sSqlCartCred + "" + sTab + "E3VALCRE, " + sTab + "E3DESCRE, " + sTab + "E3STACRE ) ";
            sSqlCartCred = sSqlCartCred + "VALUES (" + sCpdAluno + " , GETDATE() , 1 , " + dValorPago + " , " + msqCarta + " , 0)";
        }

        public void tipoServico()
        {
            if (sTipoBol == "0")
            {
                codProdutoNfe = "0001";
                bMensalidade = true;
            }
            else if (sTipoBol == "1")
            {
                codProdutoNfe = "0002";
                bNegociacao = true;
            }
            else if (sTipoBol == "2")
            {
                codProdutoNfe = "0003";
                bServico = true;
            }
            else if (sTipoBol == "3")
            {
                codProdutoNfe = "0005";
                bPreInscricao = true;
            }
            else if (sTipoBol == "3")
            {
                codProdutoNfe = "001";
                bPreInscricao = true;
            }
        }

        public void carregaClasse(SqlDataReader dados)
        {

            string data = formataData(dados["PFAXMOVDATVEN"].ToString());
            string buscar = "";
            buscar = buscar + " SELECT * FROM PFX9NOSSONUM ";
            buscar = buscar + " WHERE PFX9BANCONOSSONUM = '" + dados["PFAXMOVNOSSNUM"].ToString() + "' ";
            // caso teste na euro ou onde nao tem seu numero comentar a linha abaixo -- comentando devido erro de seu numero a api
           // buscar = buscar + "       AND PFX9SEUNUMERO = '" + dados["PFAXMOVSEUNUM"].ToString() + "' ";
            buscar = buscar + "       AND PFX9DATVEN = '" + data + "'";

            MyConnection.Conexao(sStringConexao);
            MyConnection.ComandoSQl(buscar);
            MyConnection.cmd.ExecuteNonQuery();
            SqlDataReader retornox9 = MyConnection.cmd.ExecuteReader();

            if (retornox9.HasRows)
            {
                bAchouX9 = true;
                retornox9.Read();
                sAno = retornox9["PFX9ANO"].ToString();
                sAnoSemestre = retornox9["PFX9ANOSEM"].ToString();
                sCodBol = retornox9["PFX9CODBOL"].ToString();
                if (sCodBol.Length == 1)
                {
                    sCodBol = "0" + sCodBol;
                }
                sCpdAluno = retornox9["FKX937CPD"].ToString();
                valorBase = retornox9["PFX9VALBOL"].ToString();
                sDataPagamento = formataData(dados["PFAXMOVDATPAG"].ToString());
                sDataVencimento = formataData(dados["PFAXMOVDATVEN"].ToString());
                sNossoNumero = dados["PFAXMOVNOSSNUM"].ToString();
                sSeuNumero = dados["PFAXMOVSEUNUM"].ToString();


                sTipoOcorrencia = dados["PFAXOCORRENCIA"].ToString();
                sSeqArq = dados["PFAXNUMLINHA"].ToString();
                dValorPago = Decimal.Parse(dados["PFAXMOVVALPAG"].ToString());
                sLocPag = "2";
                sDesPag = "0";
                dMulMes = 0; //Double.Parse(dados["PFAXMOVJUROSMORA"].ToString());
                sDesBan = "0";
                sNumVia = "1";
                sUsuCai = "REAL";
                sCodCam = retornox9["PFX9IES"].ToString();
                sLocTra = "0";

                if (sSeuNumero == sNossoNumero + "00")
                {
                    sTipoBol = retornox9["PFX9TIPOBOLETO"].ToString().Trim();

                    switch (sTipoBol)
                    {
                        case "5":
                            sTipoBol = "2";
                            break;

                        case "1":
                            sTipoBol = "0";
                            break;

                        case "98":
                            sTipoBol = "3";
                            break;

                        case "3":
                            sTipoBol = "1";
                            break;

                        case "4":
                            sTipoBol = "2";
                            break;

                        case "2":
                            sTipoBol = "1";
                            break;

                        case "00":
                            sTipoBol = "4";
                            break;

                        case "0":
                            sTipoBol = "0";
                            break;
                    }
                }
                else
                {
                    sTipoBol = sSeuNumero.ToString().Substring(0, 1);
                }
                if (sTipoBol == "4" || sTipoBol == "5" || sTipoBol == "6")
                {
                    sBolEmo = "2";
                }
                else { sBolEmo = "1"; }

                identBase();
                preparaStringCon();
                if (!bVestibular) {
                    busca76();
                }
                tipoServico();
            }
            MyConnection.fechaConexao();
        }

        public void identIes(string label)
        {

            switch (label)
            {
                case "UNIEURO":
                    bEuro = true;
                    break;

                case "UNICEUMA":
                    bCeuma = true;
                    break;

                case "IMPERATRIZ":
                    bCeuma = true;
                    break;

                case "FAMAZ":
                    bFamaz = true;
                    break;

                case "CEUPI":
                    bCeupi = true;
                    break;

                case "CESUP":
                    bCesup = true;
                    break;
            }
        }

        public void identBase()
        {
            string sBase = sNossoNumero;
            sBase = sBase.Substring(0, 1);
            switch (sBase)
            {
                case "1":
                    bGraduacao = true;
                    sCatalog = "BDSPFP";
                    sTab = "PF";
                    break;
                case "2":
                    sCatalog = "BDEXTENSAOP";
                    bExtensao = true;
                    sTab = "EX";
                    break;
                case "3":
                    sCatalog = "BDEXTENSAOP";
                    bExtensao = true;
                    sTab = "EX";
                    break;
                case "4":
                    sCatalog = "BDVESTIBULARP";
                    bVestibular = true;
                    sTab = "PF";
                    break;

                case "5":
                    sCatalog = "BDSCBP";
                    codProdutoNfe = "004";
                    bBiblioteca = true;
                    sTab = "PF";
                    break;

                case "7":
                    bGraduacao = true;
                    sCatalog = "BDSPFP";
                    sTab = "PF";
                    break;

                case "9":
                    sCatalog = "BDEXTENSAOP";
                    bExtensao = true;
                    sTab = "EX";
                    break;

                case "8":
                    sCatalog = "BDVESTIBULARP";
                    bVestibular = true;
                    sTab = "PF";
                    break;
            }
        }

        public void busca76()
        {
            buscar = "";
            buscar = buscar + "SELECT * FROM " + tab + "76MOVFIT";
            buscar = buscar + " WHERE " + tab + "76USUCAI = 'REAL'";
            buscar = buscar + " AND FK76X9BANCONOSSONUM = " + sNossoNumero;
            buscar = buscar + " AND FK76X9SEUNUMERO = " + sSeuNumero;
            buscar = buscar + " AND FK7637CPDALU =" + sCpdAluno;

            cConn.fecharConexao();
            cConn.abrirConexao(sStringConexao);
            SqlDataReader retorno76 = cConn.buscaDados(buscar);

            if (retorno76.HasRows)
            {
                retorno76.Read();
                sAnoReg = retorno76[tab + "76ANOREG"].ToString();
                sSeqReg = retorno76[tab + "76SEQREG"].ToString();
                bAchou76 = true;
            }
            cConn.fecharConexao();
        }

        public void busca76teste()
        {
            buscar = "";
            buscar = buscar + "SELECT * FROM " + tab + "76MOVFIT";
            buscar = buscar + " WHERE " + tab + "76USUCAI = 'REAL'";
            // buscar = buscar + " AND FK76X9BANCONOSSONUM = " + sNossoNumero;
            //buscar = buscar + " AND FK76X9SEUNUMERO = " + sSeuNumero;
            buscar = buscar + " AND FK7637CPDALU =" + sCpdAluno;

            cConn.abrirConexao(sStringConexao);
            SqlDataReader retorno76 = cConn.buscaDados(buscar);

            if (retorno76.HasRows)
            {
                retorno76.Read();
                sAnoReg = retorno76[tab + "76ANOREG"].ToString();
                sSeqReg = retorno76[tab + "76SEQREG"].ToString();
                bAchou76 = true;
            }
            cConn.fecharConexao();
        }

        public void inserirCartaCredito(ref SqlTransaction tran, ref SqlConnection con)
        {
            if (bCartaCredito)
            {
                preparaSqlCartaCred();
                SqlCommand cmd1 = con.CreateCommand();
                try
                {
                    cmd1.CommandText = sSqlCartCred;
                    cmd1.Transaction = tran;
                    cmd1.ExecuteNonQuery(); // EXECUTA CMD
                }
                catch
                {
                    bRollback = true;
                    return;
                }
            }
        }

        public void busca47()
        {
            buscar = "";
            buscar = buscar + " SELECT * ";
            buscar = buscar + "     FROM  " + sTab + "47BOLETT ";

            if (bNegociacao)
            {//@@
                buscar = buscar + " WHERE FK4746CPDALU =  " + sCpdAluno;
                buscar = buscar + " AND FK4746ANOBOL = '" + sAnoSemestre + "' ";
                buscar = buscar + " AND FK4746BOLETA = '" + sCodBol + "' ";
                //buscar = buscar + " AND "+sTab+"47SITBOL = '" + sSitBol + "' ";
                buscar = buscar + " order by " + sTab + "47DATVEN DESC ";
            }
            else
            {
                buscar = buscar + " WHERE FK4737CPDALU =  " + sCpdAluno;
                buscar = buscar + "    AND  " + sTab + "47CODBOL = '" + sCodBol + "' ";
                buscar = buscar + "    AND  " + sTab + "47ANOBOL = '" + sAnoSemestre + "' ";
                buscar = buscar + " order by " + sTab + "47DATVEN DESC ";
            }

            cConn.abrirConexao(sStringConexao);
            SqlDataReader retorno47 = cConn.buscaDados(buscar);

            if (retorno47.HasRows)
            {
                dtretorno47 = carregaTabela(retorno47);
                bAchou47 = true;
            }
            else
            {
                bAchou47 = false;
                bCartaCredito = true;
            }
            cConn.fecharConexao();
        }
        public bool recuperaNegociacao()
        {

            bool retorno = false;
            buscar = "";
            buscar = buscar + " SELECT DISTINCT " + sTab + "47CODBOL,  " + sTab + "47ANOBOL  ";
            buscar = buscar + "     FROM  LOG_" + sTab + "47BOLETT ";

            if (bNegociacao)
            {//@@
                buscar = buscar + " WHERE FK4737CPDALU =  " + sCpdAluno;
                buscar = buscar + " AND FK4746ANOBOL = '" + sAnoSemestre + "' ";
                buscar = buscar + " AND FK4746BOLETA = '" + sCodBol + "' ";
            }

            cConn.abrirConexao(sStringConexao);
            SqlDataReader retorno47 = cConn.buscaDados(buscar);

            if (retorno47.HasRows)
            {
                dtretorno47 = carregaTabela(retorno47);
                bRecuperadoNeg = true;
                retorno = true;
            }

            cConn.fecharConexao();

            return retorno;
        }

        public bool baixa47(ref SqlTransaction tran, ref SqlConnection con, DataRow row)
        {
            bool retorno = false;


            string CamposValores = "";
            CamposValores = CamposValores + " " + sTab + "47DATPAG = '" + sDataPagamento + "' ";
            CamposValores = CamposValores + ", " + sTab + "47VALMUL = " + dMulMes;

            if (bNegociacao)
            {
                CamposValores = CamposValores + ", " + sTab + "47SITBOL = '4' ";
            }
            else
            {
                CamposValores = CamposValores + ", " + sTab + "47SITBOL = '1' ";

            }

            if (bGraduacao)
            {
                CamposValores = CamposValores + ", PF76ANOREG = " + sAnoReg;
                CamposValores = CamposValores + ", PF76SEQREG = " + sSeqReg;
            }
            else
            {
                CamposValores = CamposValores + ", FK4776ANOREG = " + sAnoReg;
                CamposValores = CamposValores + ", FK4776SEQREG = " + sSeqReg;
            }

            CamposValores = CamposValores + ", " + sTab + "47VALPAG = REPLACE('" + dValorPago + "', ',' ,'.') ";
            CamposValores = CamposValores + ", " + sTab + "47OBSQUI =  '" + sMsgQ + "' ";

            string g_sql = "";
            g_sql = g_sql + " UPDATE " + sTab + "47BOLETT ";
            g_sql = g_sql + "   SET " + CamposValores;
            g_sql = g_sql + "    WHERE  FK4737CPDALU = " + sCpdAluno;
            g_sql = g_sql + "      AND " + sTab + "47CODBOL = " + row["" + sTab + "47CODBOL"].ToString();
            g_sql = g_sql + "      AND " + sTab + "47ANOBOL = " + row["" + sTab + "47ANOBOL"].ToString();

            // INSERE NA BASE... 
            SqlCommand cmd2 = con.CreateCommand();
            try
            {
                cmd2.CommandText = g_sql;
                cmd2.Transaction = tran;
                cmd2.ExecuteNonQuery(); // EXECUTA CMD
                retorno = true;
            }
            catch
            {
                bRollback = true;
            }

            return retorno;
        }
        public void baixa48(ref SqlTransaction tran, ref SqlConnection con, DataRow row)
        {
            string sQuery = "";
            sQuery = sQuery + "UPDATE   " + sTab + "48EFIMET ";
            sQuery = sQuery + "     SET      " + sTab + "48VALPAG = REPLACE('" + dValorPago + "', ',' ,'.') , ";
            sQuery = sQuery + "              " + sTab + "48DATPAG = '" + sDataPagamento + "'";
            sQuery = sQuery + "     WHERE    FK4837CPDALU = " + sCpdAluno;
            sQuery = sQuery + "              AND FK4847ANOBOL = " + row["" + sTab + "47ANOBOL"].ToString();
            sQuery = sQuery + "              AND FK4847CODBOL =  " + row["" + sTab + "47CODBOL"].ToString();

            // INSERE NA BASE... 
            SqlCommand cmd3 = con.CreateCommand();
            try
            {
                cmd3.CommandText = sQuery;
                cmd3.Transaction = tran;
                cmd3.ExecuteNonQuery(); // EXECUTA CMD
            }
            catch
            {
                bRollback = true;
            }

        }

        public void baixaD6(ref SqlTransaction tran, ref SqlConnection con)
        {

            //BUSCA NA PFD6
            sSql = "";
            sSql = sSql + "SELECT FKD652CODPRO AS CODPRO ";
            sSql = sSql + "  FROM " + sTab + "D6BOLSET ";
            sSql = sSql + "  WHERE FKD637CPDALU = " + sCpdAluno;
            sSql = sSql + "         AND " + sTab + "D6ANOBOL = " + sAnoSemestre;
            sSql = sSql + "         AND " + sTab + "D6CODBOL = " + sCodBol;

            cConn.abrirConexao(sStringConexao);
            SqlDataReader retornoD6 = cConn.buscaDados(sSql);

            if (retornoD6.HasRows)//verificar se tem registros do serviço na D6
            {
                retornoD6.Read();
                sCodProtocolo = retornoD6["CODPRO"].ToString();
                cConn.fecharConexao();

                //UPDATE NA PFD6

                string baixar = "";
                baixar = baixar + "UPDATE  " + sTab + "D6BOLSET ";
                baixar = baixar + " SET    " + sTab + "D6VALPAG      = REPLACE('" + dValorPago + "', ',' , '.') , ";
                baixar = baixar + "        " + sTab + "D6DATPAG      = '" + sDataPagamento + "', ";
                baixar = baixar + "        " + sTab + "D6VALMUL      = " + dMulMes + ", ";
                baixar = baixar + "        " + sTab + "D6DESCON      = " + sDesPag + " , ";
                baixar = baixar + "        " + sTab + "D6LOCPAG      = '" + sLocPag + "', ";
                baixar = baixar + "        FKD676ANOREG    = " + sAnoReg + ", ";
                baixar = baixar + "        FKD676SEQREG    = " + sSeqReg + ", ";
                baixar = baixar + "        " + sTab + "D6SITBOL      = '1', ";
                baixar = baixar + "        " + sTab + "D6OBSQUI      = '" + sMsgQ + "'";
                baixar = baixar + " WHERE  1=1 ";
                baixar = baixar + "        AND FKD637CPDALU = " + sCpdAluno;
                baixar = baixar + "        AND " + sTab + "D6ANOBOL = " + sAnoSemestre;
                baixar = baixar + "        AND " + sTab + "D6CODBOL = " + sCodBol;


                // INSERE NA BASE... 
                SqlCommand cmd1 = con.CreateCommand();
                try
                {
                    cmd1.CommandText = baixar;
                    cmd1.Transaction = tran;
                    cmd1.ExecuteNonQuery(); // EXECUTA CMD
                    bBaixouD6 = true;
                }
                catch
                {
                    bRollback = true;
                    return;
                }

                if (sCodProtocolo == "" || sCodProtocolo == null)
                {
                    //BUSCA NA PFD7 O COD DO PROTOCOLO
                    sSql = "";
                    sSql = sSql + " SELECT FKD752CODPRO, FKD750CODSER ";
                    sSql = sSql + " FROM " + sTab + "D7BOLDIT ";
                    sSql = sSql + " WHERE FKD737CPDALU = " + sCpdAluno;
                    sSql = sSql + "         AND FKD7D6ANOBOL = " + sAnoSemestre;
                    sSql = sSql + "         AND FKD7D6CODBOL = " + sCodBol;

                    cConn.abrirConexao(sStringConexao);
                    SqlDataReader retornoD7 = cConn.buscaDados(sSql);

                    if (retornoD7.HasRows)//verificar se tem registros do serviço na D6
                    {
                        retornoD7.Read();
                        sCodServico = retornoD7["FKD750CODSER"].ToString();
                        sCodProtocolo = retornoD7["FKD752CODPRO"].ToString();
                        cConn.fecharConexao();
                    }
                }
                cConn.fecharConexao();
            }
        }

        public void baixa52(ref SqlTransaction tran, ref SqlConnection con)
        {
            sSql = "";
            sSql = sSql + " SELECT "+sTab+"52CODPRO ";
            sSql = sSql + " FROM " + sTab + "52PROTOT ";
            sSql = sSql + " WHERE FK5237CPDALU = " + sCpdAluno;
            sSql = sSql + "         AND " + sTab + "52CODPRO = " + sCodProtocolo;

            cConn.abrirConexao(sStringConexao);
            SqlDataReader retorno52 = cConn.buscaDados(sSql);

            if (retorno52.HasRows)//verificar se tem registros do serviço na D6
            {
                baixar = baixar + "UPDATE " + sTab + "52PROTOT ";
                baixar = baixar + " SET    " + sTab + "52OBSPRO      = '" + sMsgQ + "' , ";
                baixar = baixar + "       FK5276ANOREG      = '" + sAnoReg + "', ";
                baixar = baixar + "       FK5276SEQREG      = " + sSeqReg + ", ";
                baixar = baixar + "       " + sTab + "52VALPRO      = REPLACE('" + dValorPago + "' , ',' , '.' ) ";
                baixar = baixar + " WHERE  1=1 ";
                baixar = baixar + "       AND FK5237CPDALU = " + sCpdAluno;
                baixar = baixar + "         AND " + sTab + "52CODPRO = " + sCodProtocolo;

                preparaSqlCartaCred();
                SqlCommand cmd1 = con.CreateCommand();
                try
                {
                    cmd1.CommandText = baixar;
                    cmd1.Transaction = tran;
                    cmd1.ExecuteNonQuery(); // EXECUTA CMD
                    cConn.fecharConexao();
                }
                catch
                {
                    cConn.fecharConexao();
                    bRollback = true;
                    return;
                }
            }
            cConn.fecharConexao();
        }

        public string formataData(string data)
        {
            DateTime oDate = Convert.ToDateTime(data);
            data = oDate.Year + "-" + oDate.Month + "-" + oDate.Day;
            return data;
        }

        public void busca46()
        {
            buscar = "";
            buscar = buscar + " SELECT * FROM " + sTab + "46BOLNET";
            buscar = buscar + "    WHERE   FK4637CPDALU = " + sCpdAluno;
            buscar = buscar + "    AND " + sTab + "46ANOBOL = " + sAnoSemestre;
            buscar = buscar + "    AND " + sTab + "46BOLETA = " + sCodBol;

            cConn.abrirConexao(sStringConexao);
            SqlDataReader retorno46 = cConn.buscaDados(buscar);

            if (retorno46.HasRows)
            {
                retorno46.Read();
                sSitBol = retorno46["" + sTab + "46SITBOL"].ToString();
                bAchou46 = true;
                retorno46 = null;
            }
            else
            {
                bCartaCredito = true;
            }

            cConn.fecharConexao();
        }

        public bool baixa46(ref SqlTransaction tran, ref SqlConnection con)
        {
            bool retorno = false;

            if (sSitBol == "2" || sSitBol == "3") //  NEGOCIADO
            {
                if (sCpdAluno != "" && sAnoSemestre != "" && sCodBol != "" && sSeqReg != "")
                {
                    string sQuery = "";
                    sQuery = sQuery + " UPDATE   " + sTab + "46BOLNET ";
                    sQuery = sQuery + "   SET      " + sTab + "46VALPAG = REPLACE('" + dValorPago + "', ',' ,'.') , ";
                    sQuery = sQuery + "         " + sTab + "46DATPAG = '" + sDataPagamento + "', ";
                    sQuery = sQuery + "         " + sTab + "46OBSNEG = '" + sMsgQ + "', ";
                    sQuery = sQuery + "         " + sTab + "46SITBOL = '1'";
                    sQuery = sQuery + "   WHERE    FK4637CPDALU = " + sCpdAluno;
                    sQuery = sQuery + "         AND " + sTab + "46ANOBOL = " + sAnoSemestre;
                    sQuery = sQuery + "         AND " + sTab + "46BOLETA =  " + sCodBol;

                    // UPDATE NA BASE... 
                    SqlCommand cmd1 = con.CreateCommand();
                    try
                    {
                        cmd1.CommandText = sQuery;
                        cmd1.Transaction = tran;
                        cmd1.ExecuteNonQuery(); // EXECUTA CMD
                        retorno = true;
                    }
                    catch
                    {
                        bRollback = true;
                    }
                }
            }else if (sSitBol == "1")
            {
                retorno = true;
            }
            return retorno;
        }

        public string formataAnoSemestre(string sAnoSemestre)
        {
            string semestre;
            string ano;
            ano = sAnoSemestre.Substring(0, 4);
            semestre = sAnoSemestre.Substring(4, 1);
            semestre = Convert.ToString(Int32.Parse(semestre) - 1);
            if (semestre == "0")
            {
                semestre = "2";
                ano = Convert.ToString(Int32.Parse(ano) - 1);
                ano = ano + semestre;
            }
            else
            {
                ano = ano + semestre;
            }
            return ano;
        }

        public void buscaW6Ref2(ref SqlConnection con, ref SqlTransaction tran)
        {
            buscar = "";
            buscar = buscar + " SELECT * FROM " + sTab + "W6DETVALBOL";
            buscar = buscar + "    WHERE  FKW647CPDALU = " + sCpdAluno;
            buscar = buscar + "    AND FKW647ANOBOL = " + sAnoSemestre;
            buscar = buscar + "    AND FKW647CODBOL = " + sCodBol;
            buscar = buscar + "     AND   isnull(" + sTab + "W6STATUS, 'A') not in ('F')";

            cConn.abrirConexao(sStringConexao);
            SqlDataReader retornoW6 = cConn.buscaDados(buscar);
            if (retornoW6.HasRows)
            {
                sSql = "";
                sSql = sSql + "UPDATE " + sTab + "W6DETVALBOL ";
                sSql = sSql + "     SET  " + sTab + "W6NOSSONUMERO = '" + sNossoNumero + "' ,";
                sSql = sSql + "        " + sTab + "W6BOLREG = 'S' ";
                sSql = sSql + " WHERE  FKW647CPDALU = " + sCpdAluno;
                sSql = sSql + "       AND FKW647ANOBOL = " + anoSemestre;
                sSql = sSql + "       AND FKW647CODBOL = " + sCodBol;
                sSql = sSql + "     AND   isnull(PFW6STATUS, 'A') not in ('F')";

                SqlCommand cmd1 = con.CreateCommand();
                cmd1.CommandText = sSql;
                cmd1.Transaction = tran;
                try
                {
                    cmd1.ExecuteNonQuery();
                }
                catch
                {
                    bRollback = true;
                    cConn.fecharConexao();
                    return;
                }

            }
            cConn.fecharConexao();
        }

        public void buscaW6(ref SqlConnection con, ref SqlTransaction tran)
        {
            buscar = "";
            buscar = buscar + " SELECT * FROM " + sTab + "W6DETVALBOL";
            buscar = buscar + "    WHERE  FKW647CPDALU = " + sCpdAluno;
            buscar = buscar + "    AND FKW647ANOBOL = " + sAnoSemestre;
            buscar = buscar + "    AND FKW647CODBOL = " + sCodBol;

            cConn.abrirConexao(sStringConexao);
            SqlDataReader retornoW6 = cConn.buscaDados(buscar);
            if (retornoW6.HasRows)
            {
                sSql = "";
                sSql = sSql + "UPDATE  " + sTab + "W6DETVALBOL ";
                sSql = sSql + " SET    " + sTab + "W6VALPAG = REPLACE('" + dValorPago + "' , ',' , '.' ) ,";
                sSql = sSql + "        " + sTab + "W6SITBOL = 1 ,";
                sSql = sSql + "        " + sTab + "W6STATUS = 'F' ,";
                sSql = sSql + "        " + sTab + "W6DATPAG = '" + sDataPagamento + "' ,";
                sSql = sSql + "        " + sTab + "W6NOSSONUMERO = '" + sNossoNumero + "' ,";
                sSql = sSql + "        " + sTab + "W6BOLREG = 'S' ,";
                sSql = sSql + "        " + sTab + "W6VALDIFPAGBAIXA47 = '0' ";

                sSql = sSql + " WHERE  1=1 ";
                sSql = sSql + "       AND FKW647CPDALU = " + sCpdAluno;
                sSql = sSql + "       AND FKW647ANOBOL = " + anoSemestre;
                sSql = sSql + "       AND FKW647CODBOL = " + sCodBol;

                SqlCommand cmd1 = con.CreateCommand();
                cmd1.CommandText = sSql;
                cmd1.Transaction = tran;
                cmd1.ExecuteNonQuery();

            }
            cConn.fecharConexao();
        }

        public void busca87()
        {

            buscar = "";
            buscar = buscar + " SELECT * FROM " + sTab + "87FORMOT";
            buscar = buscar + "    WHERE  FK8776ANOREG = " + sAnoSemestre;
            buscar = buscar + "    AND FK8776SEQREG = " + sCodBol;

            MyConnection.Conexao(sStringConexao);
            MyConnection.ComandoSQl(buscar);
            MyConnection.cmd.ExecuteNonQuery();
            SqlDataReader retorno87 = MyConnection.cmd.ExecuteReader();

            if (!retorno87.HasRows)
            {
                sSql = sSql + " INSERT INTO " + sTab + "87FORMOT ";
                sSql = sSql + " VALUES    ('14', '" + sAnoReg + "' , '" + sSeqReg + "' , 'REPLACE('" + dValorPago + "' , ',' , '.' )' )";

                MyConnection.ComandoSQl(baixar);
                MyConnection.cmd.ExecuteNonQuery(); // EXECUTA CMD
            }
        }

        public void busca82(string servico, ref SqlConnection con, ref SqlTransaction tran)
        {

            buscar = "";
            buscar = buscar + " SELECT * FROM " + sTab + "82EMOFIT";
            buscar = buscar + "    WHERE  FK8276ANOREG = " + sAnoSemestre;
            buscar = buscar + "    AND FK8276SEQREG = " + sSeqReg;

            cConn.abrirConexao(sStringConexao);
            SqlDataReader retorno82 = cConn.buscaDados(buscar);

            if (!retorno82.HasRows)
            {
                cConn.fecharConexao();
                sSql = sSql + "INSERT INTO " + sTab + "82EMOFIT ";
                sSql = sSql + " VALUES    (" + sAnoReg + " , " + sSeqReg + " , '" + servico + "' , '1' , 'REPLACE('" + dValorPago + "' , ',' , '.' )' )";

                SqlCommand cmd1 = con.CreateCommand();
                try
                {
                    cmd1.CommandText = sSql;
                    cmd1.Transaction = tran;
                    cmd1.ExecuteNonQuery(); // EXECUTA CMD
                }
                catch
                {
                    bRollback = true;
                    return;
                }
            }
            cConn.fecharConexao();
        }

        public void identTipoPreInscricao()
        {
            switch (sCodBol)
            {
                case "97":
                    sFormPag = "01";
                    sTipoServ = "685";
                    bMatExtensao = true;
                    break;

                case "98":
                    sFormPag = "01";
                    sTipoServ = "706";
                    break;

                case "99":
                    sFormPag = "01";
                    sTipoServ = "227";
                    break;

                default:
                    sFormPag = "01";
                    sTipoServ = "3";
                    bPreMatExtensao = true;
                    break;
            }
        }

        public void busca57()
        {
            buscar = "";
            buscar = buscar + " SELECT  *, ISNULL(EX57DATPRE, GETDATE()) AS DATPRE ";
            buscar = buscar + " FROM    EX57PRINST ";
            buscar = buscar + "LEFT JOIN EX12BOLINT ON FK1257NUMINS = EX57NUMINS, EXT8INSCUR, EX33TURMAT, EXB7CAMCUT ";
            buscar = buscar + " WHERE   EX57NUMINS          = " + sCpdAluno;
            buscar = buscar + "        AND FKT857NUMINS    = EX57NUMINS ";
            buscar = buscar + "        AND FKT833CODTUR    = EX33CODTUR ";
            buscar = buscar + "        AND FKT833ANOREF    = EX33ANOREF ";
            buscar = buscar + "        AND FKT8B7CODIGO    = EXB7CODIGO ";

            MyConnection.ComandoSQl(buscar);
            MyConnection.cmd.ExecuteNonQuery();
            SqlDataReader retorno57 = MyConnection.cmd.ExecuteReader();

            if (retorno57.HasRows)
            {//SET DADOS PRE INSCRIÇÃO
                sAchou57 = true;
            }
        }

        public void baixaVestibular(ref SqlConnection con, ref SqlTransaction tran)
        {

            buscar = "";
            buscar = buscar + "SELECT FK1501INSVES ";
            buscar = buscar + "   FROM  VT15BOLINT ";
            buscar = buscar + "   WHERE   FK1501INSVES = " + sCpdAluno;


            cConn.abrirConexao(sStringConexao);
            SqlDataReader retornoVT = cConn.buscaDados(buscar);

            if (retornoVT.HasRows)
            {
                cConn.fecharConexao();
                bAchouVestibular = true;
                sSql = "";
                sSql = sSql + "UPDATE  VT15BOLINT ";
                sSql = sSql + "     SET VT15ANOREG = " + sAnoReg;
                sSql = sSql + "     SET VT15SEQREG = " + Strings.Format(sSeqReg, "000000");
                sSql = sSql + "     SET VT15DATPAG = " + sDataPagamento;
                sSql = sSql + "     SET VT15VALPAG = REPLACE('" + dValorPago + "' , ',' , '.' )";
                sSql = sSql + "     SET VT15LOCPAG = '2'";
                sSql = sSql + "     SET VT15SITBOL = '1'";
                sSql = sSql + "     SET VT15OBSQUI = " + sMsgQ;
                sSql = sSql + "     WHERE  FK1501INSVES = " + sCpdAluno;

                SqlCommand cmd1 = con.CreateCommand();
                try
                {
                    cmd1.CommandText = sSql;
                    cmd1.Transaction = tran;
                    cmd1.ExecuteNonQuery(); // EXECUTA CMD
                    bAchouVestibular = true;
                }
                catch
                {
                    bRollback = true;
                    return;
                }
            }
            else
            {
                Console.WriteLine("Não foi encontrada inscrição : " + sCpdAluno + " || Nosso Numero" + sNossoNumero);
                bRollback = true;
                bCartaCredito = true;
                cConn.fecharConexao();
            }
        }

        public void alocarAluno(ref SqlConnection con, ref SqlTransaction tran)
        {
            buscar = "";
            buscar = buscar + "SELECT VT01INSVES";
            buscar = buscar + "      FROM VT01CANDIT";
            buscar = buscar + "      WHERE VT01INSVES =" + sCpdAluno;
            buscar = buscar + "      AND  VT01TIPVES = 'T'";

            cConn.abrirConexao(sStringConexao);
            SqlDataReader retorno = cConn.buscaDados(buscar);

            if (!retorno.HasRows)
            {
                cConn.fecharConexao();
                buscar = "";
                buscar = buscar + " SELECT FK2106ANOVES, FK2106CODVES, VT21DIAVES, FK2122CODHOR";
                buscar = buscar + "     FROM VT02CURALT, VT21HORVET";
                buscar = buscar + "     WHERE  FK0201INSVES = " + sCpdAluno;
                buscar = buscar + "     AND VT02INDOPC = '1'";
                buscar = buscar + "     AND FK2106CODVES = FK0204CODVES";
                buscar = buscar + "     AND FK2106ANOVES = FK0204ANOVES";

                cConn.abrirConexao(sStringConexao);
                retorno = cConn.buscaDados(buscar);

                if (retorno.HasRows)
                {
                    retorno.Read();
                    sAnoVestibular = retorno["FK2106ANOVES"].ToString();
                    sCodVestibular = retorno["FK2106CODVES"].ToString();
                    sDiaVestibular = retorno["VT21DIAVES"].ToString();
                    sCodHorario = retorno["FK2122CODHOR"].ToString();
                    cConn.fecharConexao();
                }

                if (!bEuro)
                {
                    buscar = "";
                    buscar = buscar + " select FK2001INSVES FROM vt20locprt " +
                            " Where FK2001INSVES = " + sCpdAluno;

                    cConn.abrirConexao(sStringConexao);
                    retorno = cConn.buscaDados(buscar);
                    if (!retorno.HasRows)
                    {
                        bAlocaAluno = true;
                        cConn.fecharConexao();
                        return;
                    }
                    buscar = "";
                    buscar = buscar + "SELECT FK0405CODCAM FROM VT02CURALT, VT04CAMCUT " +
                           " WHERE VT04CODIGO = FK0204CODIGO And FK0406CODVES = FK0204CODVES And " +
                           " FK0406ANOVES = FK0204ANOVES AND " +
                           " VT02INDOPC = 1 AND FK0201INSVES = " + sCpdAluno;

                    cConn.abrirConexao(sStringConexao);
                    retorno = cConn.buscaDados(buscar);
                    if (retorno.HasRows)
                    {
                        bAlocaAluno = false;
                        cConn.fecharConexao();
                        return;

                    }
                    else
                    {
                        retorno.Read();

                        sCampus = Strings.Format(retorno["FK0405CODCAM"].ToString(), "00");

                        if (Int32.Parse(sCampus) < 4)
                        {
                            sCampus = "01";
                        }
                    }
                    buscar = "";
                    buscar = buscar + "SELECT  VT19CODSAL, FK1918CODPRE, VT19NUMALU " +
                           " FROM    VT19SALAUT, VT18PREDIT " +
                           " WHERE   1=1 " +
                           "        AND FK1918CODPRE    = VT18CODPRE " +
                           "        AND VT19TIPSAL      = 'S' " +
                           "        AND FK1805CODCAM = '" + sCampus + "'" +
                           "        AND FK1918CODPRE IN ('02','03','04') " +
                           "        AND VT19CODSAL IN ('06','07','08','09','18','19','20','21','22','23','24','43','44','45','46','67','68','69','70','71','72','73','74','75','76') " +
                           " ORDER BY FK1918CODPRE, VT19CODSAL ";

                    cConn.abrirConexao(sStringConexao);
                    retorno = cConn.buscaDados(buscar);

                    if (retorno.HasRows)
                    {
                        DataTable retornoDt = carregaTabela(retorno);
                        cConn.fecharConexao();
                        foreach (DataRow row in retornoDt.Rows)
                        {
                            int numAlu = Int32.Parse(Strings.Trim(row["VT19NUMALU"].ToString()));

                            buscar = "";
                            buscar = buscar + "Select count(*) as QuantCand from vt20locprt " +
                                   " where fk2021diaves = '" + sDiaVestibular + "' and " +
                                   " fk2021codhor = '" + sCodHorario + "' and " +
                                   "  FK2019CODSAL = '" + Strings.Trim(row["VT19CODSAL"].ToString()) + "' and " +
                                   " FK2019CODPRE = '" + Strings.Trim(row["FK1918CODPRE"].ToString()) + "'";

                            cConn.abrirConexao(sStringConexao);
                            SqlDataReader retorno2 = cConn.buscaDados(buscar);
                            if (retorno2.HasRows)
                            {
                                retorno2.Read();
                                int quantCand = Int32.Parse(Strings.Trim(retorno2["QuantCand"].ToString()));

                                if (quantCand < numAlu)
                                {
                                    sSql = "";
                                    sSql = sSql + " insert into vt20locprt " +
                                                  " (fk2001insves, fk2021codves, fk2021anoves, fk2021diaves, fk2021codhor,FK2019CODSAL,FK2019CODPRE) " +
                                                  " values (" + sCpdAluno + "," + sCodVestibular + "," +
                                                  sAnoVestibular + ",'" + sDiaVestibular + "','" + sCodHorario +
                                                  "','" + Strings.Trim(row["VT19CODSAL"].ToString()) + "','" +
                                                  Strings.Trim(row["FK1918CODPRE"].ToString()) + "')";

                                    SqlCommand cmd1 = con.CreateCommand();
                                    try
                                    {
                                        cmd1.CommandText = sSql;
                                        cmd1.Transaction = tran;
                                        cmd1.ExecuteNonQuery(); // EXECUTA CMD
                                        cConn.fecharConexao();
                                        bInseriu20 = true;
                                    }
                                    catch
                                    {
                                        cConn.fecharConexao();
                                        bRollback = true;
                                        return;
                                    }
                                }
                            }
                        }
                    }
                }
                else
                {

                    bAlocaAluno = true;
                    return;
                }
            }
        }

        static string GetMd5Hash(MD5 md5Hash, string input)
        {
            byte[] data = md5Hash.ComputeHash(Encoding.UTF8.GetBytes(input));

            StringBuilder sBuilder = new StringBuilder();

            for (int i = 0; i < data.Length; i++)
            {
                sBuilder.Append(data[i].ToString("x2"));
            }

            return sBuilder.ToString();
        }

        public void retornaTurno(string turnos)
        {
            sTurno = turnos;
            switch (sTurno)
            {
                case "1":
                    sCodTurno = "M";
                    break;
                case "2":
                    sCodTurno = "V";
                    break;
                case "3":
                    sCodTurno = "N";
                    break;
                case "4":
                    sCodTurno = "D";
                    break;
                case "5":
                    sCodTurno = "I";
                    break;
                case "":
                    sCodTurno = "M";
                    break;
            }
        }

        public void buscaturmaExtensao(string turma)
        {
            if (turma != "")
            {
                buscar = "";
                buscar = buscar + " SELECT FK01D0ITETAB";
                buscar = buscar + "      FROM EX01CURSOT, EX33TURMAT, EXB7CAMCUT ";
                buscar = buscar + "   WHERE EX01CODIGO   = FKB701CODIGO ";
                buscar = buscar + "   AND FK33B7CODIGO = EXB7CODIGO ";
                buscar = buscar + "   AND EX33CODTUR   = " + turma;

                cConn.abrirConexao(sStringConexao);
                SqlDataReader retornotur = cConn.buscaDados(buscar);

                if (retornotur.HasRows)
                {
                    bTurmaExtensao = true;
                    cConn.fecharConexao();

                }
                cConn.fecharConexao();
            }
        }

        public void carregaVarNovoALuno(DataRow retorno)
        {

            sCodigoCurso = retorno["FKB701CODIGO"].ToString();
            sNomeAluno = retorno["ex57nomeal"].ToString();
            sEnderecoAluno = retorno["EX57ENDERE"].ToString();
            sBairroAluno = retorno["EX57BAIRRO"].ToString();
            sCidadeAluno = retorno["EX57CIDADE"].ToString();
            sUnifed = retorno["EX57UNIFED"].ToString();
            sCepAluno = retorno["EX57NUMCEP"].ToString();
            sEmailAluno = retorno["EX57EMAIL"].ToString();
            sTel1Aluno = retorno["EX57TELEFR"].ToString();
            sTel2Aluno = retorno["EX57TELEFC"].ToString();
            sCelAluno = retorno["EX57TELCEL"].ToString();
            //sTurno
            sIdentAluno = retorno["EX57NUMIDE"].ToString();
            sOrgErgAluno = retorno["EX57ORGERG"].ToString();
            sCpfAluno = retorno["EX57NUMCPF"].ToString();
            sDataNascAluno = retorno["EX57DATNAS"].ToString();
            sNaturalAluno = retorno["EX57NATURA"].ToString();
            sSexoAluno = retorno["EX57IDSEXO"].ToString();
            sCpdMatAluno = retorno["EX57CPDMAT"].ToString();
            sIndFunAluno = retorno["EX57INDFUN"].ToString();
            sIndProAluno = retorno["EX57INDPRO"].ToString();
            sCrGradAluno = retorno["EX57CRGRAD"].ToString();
            sLocGraAluno = retorno["EX57LOCGRA"].ToString();
            // =  retorno["EX57NUMINS"].ToString();
            sCodTurma = retorno["EX33CODTUR"].ToString();
            sPlanoFinPos = retorno["FKT8H6CODPLA"].ToString();
        }

        public void geraAlunoPos(ref SqlTransaction tran, ref SqlConnection con)
        {
            geraCpdAluno(ref tran, ref con);

            if (sCpdAluno == "")
            {
                bRollback = true;
                Console.WriteLine("Erro ao gerar CPD da Inscrição: " + sNumInscricao);
                return;
            }
            else
            {
                if (sCodigoCurso != "")
                {
                    geraMatriculaAluno();

                    if (sMatriculaAluno == null)
                    {
                        bRollback = true;
                        Console.WriteLine("Erro ao gerar Matricula da Inscrição: " + sNumInscricao);
                        return;
                    }

                    if (sPlanoFinPos == "" || sPlanoFinPos == null)
                    {
                        buscaPlanoFinanceiro();
                    }
                    string sSenha = "";
                    if (!bEuro)
                    {
                        sSenha = "íëéçåã  ";
                    }
                    else
                    {
                        using (MD5 md5Hash = MD5.Create())
                        {
                            sSenha = GetMd5Hash(md5Hash, sSenha);
                        }
                    }

                    sSql = "";
                    sSql = sSql + "INSERT   INTO    EX37alunot "
                    + " (EX37cpdalu, EX37SENHAC,EX37matalu, EX37nomeal, EX37endere,EX37bairro,EX37cidade,EX37unifed,EX37email, "
                    + "EX37telefr,EX37telefc,EX37celula,EX37numcep,EX37cdturm, EX37cturno, EX37numide, EX37orgerg, EX37numcpf, "
                    + "EX37datnas, EX37cidnas, EX37idsexo, EX37peratu, EX37datcad, "
                    + "EX37indmat,EX37anomar,EX37anoref,EX37codper, "
                    + "EX37numrep, EX37cpdant, EX37indpro, EX37crgrad, EX37locgra, EX37indfun, FK3757NUMINS, "
                    + "FK3764CODBAN, EX37INDCER "
                    + ") values ("
                    + "" + sCpdAluno + ", "
                    + "'" + sSenha + "', "
                    + "'" + sMatriculaAluno + "', "
                    + "'" + sNomeAluno + "',"
                    + "'" + sEnderecoAluno + "',"
                    + "'" + sBairroAluno + "',"
                    + "'" + sCidadeAluno + "',"
                    + "'" + sUnifed + "',"
                    + "'" + email + "',"
                    + "'" + sTel1Aluno + "',"
                    + "'" + sTel2Aluno + "',"
                    + "'" + sCelAluno + "',"
                    + "'" + sCepAluno + "',";

                    if (sTurma == "" || sTurma == null)
                    {
                        sSql = sSql + " NULL ,";
                    }
                    else
                    {
                        sSql = sSql + "'" + sTurma + "',";
                    }

                    if (sCodTurno == "" || sCodTurno == null)
                    {
                        sSql = sSql + " NULL ,";
                    }
                    else
                    {
                        sSql = sSql + "'" + sCodTurno + "',";
                    }

                    sSql = sSql + "'" + sIdentAluno + "',"
                    + "'" + sOrgErgAluno + "',"
                    + "'" + sCpfAluno + "',";

                    if (sDataNascAluno == "" || sDataNascAluno == null)
                    {
                        sSql = sSql + " '' ,";
                    }
                    else
                    {
                        sDataNascAluno = formataData(sDataNascAluno);
                        sSql = sSql + "'" + sDataNascAluno + "',";// formataar data
                    }


                    sSql = sSql + "'" + sNaturalAluno + "',";


                    if (sSexoAluno == "" || sSexoAluno == null)
                    {
                        sSql = sSql + " 'M' ,";
                    }
                    else
                    {
                        sSql = sSql + "'" + sSexoAluno + "',";
                    }
                    sSql = sSql + "1,"
                    + " CURRENT_TIMESTAMP ,"
                    + Strings.Right(sAnoSemestre, 1) + ", "
                    + Strings.Left(sAnoSemestre, 5) + ", "
                    + Strings.Left(sAnoSemestre, 5) + ", "
                    + "'" + sPlanoFinPos + "', "
                    + "0,"
                    + " NULL , "
                    + "'" + sIndProAluno + "',"
                    + "'" + Strings.Left(sCrGradAluno, 60) + "'" + ","
                    + "'" + sLocGraAluno + "' ,"
                    + "'" + sIndFunAluno + "' ,"
                    + "'" + sNumInscricao + "' ,"
                    + " '033', 'S')";

                    SqlCommand cmd = con.CreateCommand();
                    try
                    {
                        cmd.CommandText = sSql;
                        cmd.Transaction = tran;
                        cmd.ExecuteNonQuery(); // EXECUTA CMD

                    }
                    catch
                    {
                        bRollback = true;
                        Console.WriteLine("Erro ao gravar dados na EX37 CPD:" + sCpdAluno);
                        return;
                    }

                    if (sCpdAluno != null)
                    {
                        gravarSituacao();
                        //(rsConsulta, sCpdAlu, sMatAlu, "00", sSituacaoAcad, Left$(sAnoSem, 4), Right$(sAnoSem, 1), FG_Data_Sistema, "Gerado pela pre-inscricao", 1, "", "", "") = False Then
                    }
                    else
                    {
                        bRollback = true;
                        Console.WriteLine("Erro ao gravar situação acadêmica pré-matrícula");
                        return;
                    }

                    if (sNumInscricao != "")
                    {
                        marcaMigracaoPos(ref tran, ref con);
                    }

                }
                else
                {
                    bRollback = true;
                    Console.WriteLine("Erro ao gerar CPD da Inscrição: " + sNumInscricao);
                    return;
                }

            }
        }

        public void marcaMigracaoPos(ref SqlTransaction tran, ref SqlConnection con)
        {
            sSql = "";
            sSql = sSql + " Update ex57prinst "
            + " set "
            + " ex57indmig = 'V', "
            + " ex57datcad = SYSDATETIME() "
            + " where "
            + " ex57numins = '" + sNumInscricao + "' ";

            SqlCommand cmd = con.CreateCommand();
            try
            {
                cmd.CommandText = sSql;
                cmd.Transaction = tran;
                cmd.ExecuteNonQuery(); // EXECUTA CMD

            }
            catch
            {
                bRollback = true;
            }
        }

        public void gravarSituacao()
        {


        }

        public void geraCpdAluno(ref SqlTransaction tran, ref SqlConnection con)
        {
            buscar = "";
            buscar = buscar + " SELECT EXA4SEQTAB  FROM EXA4SEQTAT WHERE EXA4NOMTAB = 'EX37ALUNOT'";

            cConn.abrirConexao(sStringConexao);

            SqlDataReader retornoCpd = cConn.buscaDados(buscar);

            if (retornoCpd.HasRows)
            {
                retornoCpd.Read();
                sCpdAluno = retornoCpd["EXA4SEQTAB"].ToString();
                cConn.fecharConexao();
            }
            else
            {
                sCpdAluno = "1";
            }

            sSql = "";
            sSql = sSql + " UPDATE EXA4SEQTAT  SET EXA4SEQTAB = EXA4SEQTAB + 1 ";
            sSql = sSql + " WHERE EXA4NOMTAB = 'EX37ALUNOT'";


            SqlCommand cmd = con.CreateCommand();
            try
            {
                cmd.CommandText = sSql;
                cmd.Transaction = tran;
                cmd.ExecuteNonQuery(); // EXECUTA CMD
            }
            catch
            {
                bRollback = true;
            }

        }

        public void geraMatriculaAluno()
        {
            string valAno;
            string valVestibular;
            string valFaixa;

            // A partir de 2005.2
            //'UniEuro
            //    'Vestibular     000     até     499
            //    'Transferido    500     até     799
            //    'Graduado       800     até     899
            //    'TranfCampi     900     até     999
            //'UniCeuma
            //    'Vestibular     000     até     299
            //    'Transf.Curso   400     até     499
            //    'Transferido    500     até     599
            //    'Graduado       600     até     699
            //    'TranfCampi     900     até     999

            if (sCampus == null) { sCampus = "1"; }
            valAno = Strings.Right(sAno, 2);
            valVestibular = retornaVestEquivalente();
            cConn.fecharConexao();
            valFaixa = "001";
            sMatriculaAluno = valAno + valFaixa + sTurno + valVestibular + "C" + sCampus;
        }

        public string retornaVestEquivalente()
        {
            string retorno = "";
            string eVest = sAnoSemestre.Substring(3, 2);
            switch (eVest)
            {
                case "92":
                    retorno = "0";
                    break;
                case "01":
                    retorno = "1";
                    break;
                case "02":
                    retorno = "2";
                    break;
                case "11":
                    retorno = "A";
                    break;
                case "12":
                    retorno = "B";
                    break;
                case "21":
                    retorno = "K";
                    break;
                case "22":
                    retorno = "L";
                    break;
                case "31":
                    retorno = "U";
                    break;
                case "32":
                    retorno = "V";
                    break;
                default:
                    retorno = "0";
                    break;
            }
            return retorno;
        }

        public void buscaPlanoFinanceiro()
        {
            SqlDataReader retornoPlanFin = null;

            buscar = "";
            buscar = buscar + " SELECT EX33codtur, FK33H6CODPLA from EX33TURMAT, EXB7CAMCUT ";
            buscar = buscar + "     where EX33ANOREF = " + sAnoSemestre + " and ";
            buscar = buscar + "           FKB7B5CODCAM = '" + sCampus + "' and ";
            buscar = buscar + "           fkb701codigo = '" + sCursoAluno + "' and ";
            buscar = buscar + "           ex33cturno   =  " + sTurno + " and ";
            buscar = buscar + "           fk33b7codigo = exb7codigo and FK33H6CODPLA is not null and ";

            sSql = "";
            sSql = sSql + "    (ex33statur is null or ex33statur ='N')";

            cConn.abrirConexao(sStringConexao);
            retornoPlanFin = cConn.buscaDados(buscar + sSql);

            if (retornoPlanFin.HasRows)
            {
                retornoPlanFin.Read();
                sPlanoFinPos = retornoPlanFin["FK33H6CODPLA"].ToString();
                sTurma = retornoPlanFin["EX33CODTUR"].ToString();
                cConn.fecharConexao();
                return;
            }
            else
            {
                cConn.fecharConexao();
                sSql = "";
                sSql = sSql + "   (ex33statur is null or ex33statur ='V') ";

                cConn.abrirConexao(sStringConexao);
                retornoPlanFin = cConn.buscaDados(buscar + sSql);

                if (retornoPlanFin.HasRows)
                {
                    retornoPlanFin.Read();
                    sPlanoFinPos = retornoPlanFin["FK33H6CODPLA"].ToString();
                    sTurma = retornoPlanFin["EX33CODTUR"].ToString();
                    cConn.fecharConexao();
                    return;

                }
                else
                {
                    cConn.fecharConexao();
                    string anoTemp;
                    int aux;

                    if (Strings.Right(sAnoSemestre, 1) == "2")
                    {
                        anoTemp = Strings.Mid(sAnoSemestre, 1, 4) + "1";

                    }
                    else if (sAnoSemestre.Substring(sAnoSemestre.Length - 1) == "1")
                    {
                        aux = Int32.Parse(Strings.Mid(sAnoSemestre, 0, 4)) - 1;
                        anoTemp = aux + "2";
                    }

                    sSql = "";
                    sSql = sSql + " (ex33statur is null or ex33statur ='N') ";

                    cConn.abrirConexao(sStringConexao);
                    retornoPlanFin = cConn.buscaDados(buscar + sSql);

                    if (retornoPlanFin.HasRows)
                    {
                        retornoPlanFin.Read();
                        sPlanoFinPos = retornoPlanFin["FK33H6CODPLA"].ToString();
                        sTurma = retornoPlanFin["EX33CODTUR"].ToString();
                        cConn.fecharConexao();
                        return;

                    }
                    else
                    {
                        cConn.fecharConexao();
                        sSql = "";
                        sSql = sSql + " (ex33statur is null or ex33statur ='V')  ";

                        cConn.abrirConexao(sStringConexao);
                        retornoPlanFin = cConn.buscaDados(buscar + sSql);
                        if (retornoPlanFin.HasRows)
                        {
                            retornoPlanFin.Read();
                            sPlanoFinPos = retornoPlanFin["FK33H6CODPLA"].ToString();
                            sTurma = retornoPlanFin["EX33CODTUR"].ToString();
                            cConn.fecharConexao();
                        }
                        else
                        {
                            sPlanoFinPos = "";
                            sTurma = "";
                            cConn.fecharConexao();
                            return;

                        }
                    }

                }

            }

        }

        public bool eAlunoPos()
        {
            bool ealuno = false;
            buscar = "";
            buscar = buscar + "SELECT  FK01D0ITETAB    AS TIPOCURSO " +
                              " FROM    EX01CURSOT, EX34CURALT, " +
                              "         EX37ALUNOT, EXB7CAMCUT  " +
                              " WHERE   1=1 " +
                              "         AND EX37CPDALU = FK3437CPDALU " +
                              "         AND EXB7CODIGO = FK34B7CODIGO " +
                              "         AND FKB701CODIGO = EX01CODIGO   " +
                              "         AND EX37CPDALU = " + sCpdAluno;

            cConn.abrirConexao(sStringConexao);
            SqlDataReader retornoEPos = cConn.buscaDados(buscar);

            if (retornoEPos.HasRows)
            {
                retornoEPos.Read();
                if (Strings.Trim("" + retornoEPos["TipoCurso"]) == "1")
                {
                    ealuno = true;
                    cConn.fecharConexao();
                }
            }
            retornoEPos = null;
            cConn.fecharConexao();
            return ealuno;
        }

        public bool geraBoletoExtensao()
        {
            bool retorno = false;

            if (bEuro || bFamaz)
            {
                dValorPago = 0;
                //ver depois isso aqui
            }
            else
            {
                dValorPago = dValorRecebido;
            }

            codBol = "01";

            if (eAlunoPos())
            {
                retorno = true;
                buscar = "";
                buscar = " SELECT  FK4737CPDALU " +
                         " FROM    EX47BOLETT " +
                         " WHERE   FK4737CPDALU    = " + sCpdAluno +
                         "        AND EX47CODBOL      = " + codBol +
                         "        AND EX47ANOBOL      = " + sAnoSemestre +
                         "        AND EXISTS ( " +
                         "              SELECT  FK4837CPDALU " +
                         "              FROM    EX48EFIMET " +
                         "              WHERE   1=1 " +
                         "                      AND FK4837CPDALU = FK4737CPDALU " +
                         "                      AND FK4847CODBOL = EX47CODBOL " +
                         "                      AND FK4847ANOBOL = EX47ANOBOL ) ";

                MyConnection.ComandoSQl(buscar);
                MyConnection.cmd.ExecuteNonQuery();
                SqlDataReader retorno4748 = MyConnection.cmd.ExecuteReader();

                if (retorno4748.HasRows)
                {
                    sSql = "";
                    sSql = "UPDATE  EX47BOLETT " +
                            "         SET     EX47VALPAG = " + dValorPago + ",  EX47DATPAG = '" + sDataPagamento + "', " +
                             "        EX47LOCPAG = '" + sLocPag + "',  EX47SITBOL = '2', ";
                    if (sSeqReg != "0" || sSeqReg != "" || sSeqReg != null)
                    {
                        sSql = sSql + "      EX76ANOREG = " + sAno + ", ";
                        sSql = sSql + "      EX76SEQREG = " + sSeqReg + ", ";
                        sSql = sSql + "      EX47LOCPAG = 'QUITADO PELA PRE-INSCRICAO-BANCO', ";
                        sSql = sSql +
                                      "WHERE   1=1 " +
                                      "        AND FK4737CPDALU    = " + sCpdAluno +
                                      "        AND EX47CODBOL      = '" + sCodBol + "' " +
                                      "        AND EX47ANOBOL      = " + sAnoSemestre;
                        MyConnection.ComandoSQl(buscar);
                        MyConnection.cmd.ExecuteNonQuery();

                        sSql = "UPDATE  EX48EFIMET " +
                               "SET     EX48VALPAG = " + dValorPago + ", EX48DATPAG = '" + sDataPagamento + "' " +
                               "WHERE   1=1 " +
                               "        AND FK4837CPDALU = " + sCpfAluno +
                               "        AND FK4847CODBOL = '" + sCodBol +
                               "        AND FK4847ANOBOL = " + sAnoSemestre;

                        MyConnection.ComandoSQl(buscar);
                        MyConnection.cmd.ExecuteNonQuery();

                    }

                }
            }
            else
            {
                retorno = false;
            }

            return retorno;
        }

        public void quitaBoletoExtensao()
        {

        }

        public void matriculaAlunoPos(string sAnoCurso, string sSemestre, string sTurmaAtu, int iTurno, int nIndMat)
        {
            buscar = "";
            buscar = buscar + "Select * From EX70MATRIT Where Fk7037CPDALU = " + sCpdAluno +
                              " AND EX70ANOMAT = " + sAnoCurso + Strings.Left(sSemestre, 1);

            MyConnection.ComandoSQl(buscar);
            MyConnection.cmd.ExecuteNonQuery();
            SqlDataReader retorno70 = MyConnection.cmd.ExecuteReader();
            if (!retorno70.HasRows)
            {
                string sIndDis = "X";
                sSqlCampos = "";
                sSqlCampos = " EX70DATAAS, FK7037CPDALU, EX70NIVELM, EX70TURMAM, EX70ANOMAT, EX70INDDIS, EX70MATMAN ";

                sSqlValores = " SYSDATETIME() , " +
                sCpdAluno + " , 1 , " + sTurmaAtu + ", '" +
                sAnoCurso + Strings.Left(sSemestre, 1) + "' , '" + sIndDis + "', NULL ";


                sSql = "";
                sSql = sSql + "INSERT INTO EX70MATRIT (" + sSqlCampos + ") values (" + sSqlValores + ")";

                MyConnection.ComandoSQl(sSql);
                MyConnection.cmd.ExecuteNonQuery();

            }
            else
            {
                string DataAs = retorno70["EX70DATAAS"].ToString();
                string NivelM = retorno70["EX70NIVELM"].ToString();
                string AnoMat = retorno70["EX70ANOMAT"].ToString();

                //atualiza Turma e Período para a nova Turma
                sSql = "";
                sSql = sSql + "Update EX70MATRIT set" +
                   "   EX70DATAAS   = SYSDATETIME() , " +
                   "   FK7037CPDALU =  " + sCpdAluno + " , " +
                   "   EX70NIVELM   = 1, " +
                   "   EX70TURMAM   =  " + sTurmaAtu + " , " +
                   "   EX70ANOMAT   = '" + sAnoCurso + Strings.Left(sSemestre, 1) + "', " +
                   "   EX70MATMAN   =  null) " +
                   " Where " +
                   "     FK7037CPDALU = " + sCpdAluno +
                   " AND EX70ANOMAT   = '" + sAnoCurso + Strings.Left(sSemestre, 1) + "'";

                MyConnection.ComandoSQl(sSql);
                MyConnection.cmd.ExecuteNonQuery();

            }

            buscar = "";
            buscar = buscar + "SELECT EX37INDASS, EX37CODPER, EX37ANOREF from EX37ALUNOT where EX37CPDALU = " + sCpdAluno;
            MyConnection.ComandoSQl(buscar);
            MyConnection.cmd.ExecuteNonQuery();
            SqlDataReader retorno37 = MyConnection.cmd.ExecuteReader();

            if (retorno37.HasRows)
            {
                string lAssinou = "S";
                string sCodPer = retorno37["EX37CODPER"].ToString();
                string sAnoPer = retorno37["EX37ANOREF"].ToString();

                sSql = "";
                sSql = sSql + "update " +
                "   EX37ALUNOT " +
                "set " +
                "   EX37INDASS = '" + lAssinou + "', " +
                "   EX37DATASS = SYSDATETIME() , " +
                "   EX37PERATU = 1, " +
                "   EX37ANOMAR = " + sAnoCurso + Strings.Left(sSemestre, 1) + ", " +
                "   EX37CTURNO = " + iTurno +
                "   where " +
                "        EX37CPDALU = " + sCpdAluno +
                "   and  EX37DATASS is null " +
                "   and (EX37INDASS = 'N' or EX37INDASS is null)";

                MyConnection.ComandoSQl(sSql);
                MyConnection.cmd.ExecuteNonQuery();
            }

            buscar = "";
            buscar = buscar + "Select *,CONVERT(varchar,EX36DATINI,121) as EXDATINI "
            + "from "
            + "   EX36ACAALT "
            + "where "
            + "       FK3637CPDALU = " + sCpdAluno
            + "   And EX36INDOFI   = 'V' "
            + "   And FK3635CODACA = '01' "
            + "   And EX36ANOREF   = " + sAnoCurso + Strings.Left(sSemestre, 1)
            + "   And EX36INDFER   = 'N'";

            MyConnection.ComandoSQl(buscar);
            MyConnection.cmd.ExecuteNonQuery();
            SqlDataReader retorno36 = MyConnection.cmd.ExecuteReader();
            if (!retorno36.HasRows)
            {
                buscar = "";
                buscar = " Select * "
                + "from "
                + "   EX36ACAALT "
                + "where "
                + "       FK3637CPDALU = " + sCpdAluno
                + "   And EX36INDOFI   = 'V' "
                + "   And EX36INDFER   = 'N' ";

                MyConnection.ComandoSQl(buscar);
                MyConnection.cmd.ExecuteNonQuery();
                SqlDataReader retorno362 = MyConnection.cmd.ExecuteReader();
                if (retorno362.HasRows)
                {
                    sSql = ""
                    + "Update "
                    + "   EX36ACAALT "
                    + "set "
                    + "   EX36INDOFI = Null "
                    + "where "
                    + "       FK3637CPDALU = " + sCpdAluno
                    + "   And EX36INDOFI   = 'V' "
                    + "   And EX36INDFER   = 'N' ";

                    MyConnection.ComandoSQl(sSql);
                    MyConnection.cmd.ExecuteNonQuery();

                }

                sSql = "";
                sSql = sSql + "INSERT INTO "
                + "   EX36ACAALT ("
                + "   FK3635CODACA, "
                + "   FK3637CPDALU, "
                + "   EX36DATINI, "
                + "   EX36ANOREF, "
                + "   EX36CODNIV, "
                + "   EX36INDOFI, "
                + "   EX36CTURNO, "
                + "   EX36CTURM, "
                + "   EX36INDFER) "
                + "VALUES ("
                + "  '01', "
                + "   " + sCpdAluno + ", "
                + "  SYSDATETIME() ,"
                + "   " + Strings.Trim(sAnoCurso) + Strings.Left(sSemestre, 1) + ", "
                + "   " + Strings.Format(1, "00") + ", "
                + "'V', "
                + "   " + iTurno + ", "
                + "   " + sTurmaAtu + ", "
                + "   'N')";

                MyConnection.ComandoSQl(sSql);
                MyConnection.cmd.ExecuteNonQuery();

            }
            else
            {

                sSql = "";
                sSql = sSql + " Update "
                + "   EX36ACAALT "
                + " set "
                + "   EX36INDOFI = null "
                + " where FK3637CPDALU = " + sCpdAluno;

                MyConnection.ComandoSQl(sSql);
                MyConnection.cmd.ExecuteNonQuery();

                sSql = "";
                sSql = sSql + " Update "
                + "   EX36ACAALT "
                + " set "
                + "   EX36DATINI = getdate(), EX36INDOFI = 'V' "
                + " where 1=1"
                + "   And FK3637CPDALU = " + sCpdAluno
                + "   And EX36DATINI   = '" + retorno36["EXDATINI"].ToString() + "' ";

                MyConnection.ComandoSQl(sSql);
                MyConnection.cmd.ExecuteNonQuery();

            }

            buscar = "";
            buscar = buscar + " select * "
            + " from "
            + "   EX33TURMAT "
            + " where "
            + "       EX33CODTUR = " + sTurmaAtu
            + "   and EX33ANOREF = " + Strings.Trim(sAnoCurso) + Strings.Left(sSemestre, 1);

            MyConnection.ComandoSQl(buscar);
            MyConnection.cmd.ExecuteNonQuery();
            SqlDataReader retorno33 = MyConnection.cmd.ExecuteReader();
            if (retorno33.HasRows)
            {
                sSql = "";
                sSql = sSql + "update "
                + "   EX37ALUNOT "
                + "set "
                + "    EX37ANOREF = " + Strings.Trim(sAnoCurso) + Strings.Left(sSemestre, 1)
                + "   ,EX37CDTURM = " + sTurmaAtu + " "
                + "   ,EX37CTURNO = " + iTurno + " "
                + "   where "
                + "   EX37CPDALU = " + sCpdAluno;

                MyConnection.ComandoSQl(sSql);
                MyConnection.cmd.ExecuteNonQuery();

            }
            else
            {
                Console.WriteLine("Turma com período indefinido. Por favor contactar o CPD." + sCpdAluno);
            }
        }

        public DataTable carregaTabela(SqlDataReader reader)
        {
            DataTable tbRetorno = new DataTable();
            tbRetorno.Load(reader);
            return tbRetorno;
        }

        public void baixaPreInscricao()
        {

            //Conn de buscas
            cConn.abrirConexao(sStringConexao);

            sNumInscricao = sCpdAluno;
            if (sNumInscricao == null || sNumInscricao == "")
            {
                Console.WriteLine("Sem Valores para busca de Inscrição");
                return;
            }
            else
            {
                buscar = "";
                buscar = buscar + "SELECT  *, ISNULL(EX57DATPRE, GETDATE()) AS DATPRE";
                buscar = buscar + "      FROM    EX57PRINST LEFT JOIN EX12BOLINT ON FK1257NUMINS = EX57NUMINS, EXT8INSCUR, EX33TURMAT, EXB7CAMCUT ";
                buscar = buscar + "      WHERE   EX57NUMINS          = " + sNumInscricao;
                buscar = buscar + "      AND FKT857NUMINS    = EX57NUMINS ";
                buscar = buscar + "      AND FKT833CODTUR    = EX33CODTUR ";
                buscar = buscar + "      AND FKT833ANOREF    = EX33ANOREF ";
                buscar = buscar + "      AND FKT8B7CODIGO    = EXB7CODIGO ";
            }

            cConn.abrirConexao(sStringConexao);
            SqlDataReader retornoIns = cConn.buscaDados(buscar);

            if (!retornoIns.HasRows)
            {
                Console.WriteLine("Numero de incrição não encontrado. Nº: " + sCpdAluno);
                cConn.fecharConexao();

            }
            else
            {
                DataTable tabelaIns = carregaTabela(retornoIns);
                cConn.fecharConexao();

                foreach (DataRow row in tabelaIns.Rows)
                {
                    ConnInsert classCon = new ConnInsert();
                    SqlConnection con = classCon.abrirConexao(sStringConexao);
                    SqlTransaction tranPreIns = con.BeginTransaction();

                    if (row["EX57INDMIG"].ToString() == "V" && row["EX12SITBOL"].ToString() == "1")
                    {
                        Console.WriteLine("Incrição já paga e migrada Nº: " + sNumInscricao);
                        rollback = true;
                    }
                    else
                    {
                        buscar = "";
                        buscar = buscar + "SELECT  ISNULL(SUM(ISNULL(EXT8VALBAS,0)),0)     AS VALOR ";
                        buscar = buscar + "        FROM    EXT8INSCUR";
                        buscar = buscar + "        WHERE   FKT857NUMINS        = " + sNumInscricao;

                        cConn.abrirConexao(sStringConexao);
                        SqlDataReader retornoVal = cConn.buscaDados(buscar);

                        if (retornoVal.HasRows)
                        {
                            retornoVal.Read();
                            string valor;
                            valor = retornoVal["VALOR"].ToString();
                            cConn.fecharConexao();

                            if (valor == "0")
                            {
                                Console.WriteLine("Valor da Inscrição igual a 0 N°" + sNumInscricao);
                                bRollback = true;
                                return;
                            }
                            else
                            {
                                sPercentualPagamento = dValorPago / Convert.ToDecimal(valor);
                                sCodCurso = row["FKT8B7CODIGO"].ToString();
                                retornaTurno(row["EX33CTURNO"].ToString());
                                buscaturmaExtensao(row["EX33CODTUR"].ToString());
                                carregaVarNovoALuno(row);
                                sCpdAluno = "";

                                if (bMatExtensao || bPreMatExtensao || bTurmaExtensao)
                                {
                                    //sPlanoFinPos = "01";
                                    geraAlunoPos(ref tranPreIns, ref con);
                                }
                                else
                                {
                                    //sPlanoFinPos = "32";
                                    geraAlunoPos(ref tranPreIns, ref con);
                                }

                                if (sCpdAluno == "")
                                {
                                    Console.WriteLine("CPD não gerado Inscrição N°" + sNumInscricao);
                                    bRollback = true;
                                }
                                else
                                {
                                    sDesPag = "0";
                                    sDesBan = "0";
                                    sBolEmo = "1";

                                    Decimal dValorRecebido = Decimal.Parse(row["EXT8VALBAS"].ToString()) * sPercentualPagamento;

                                    sSql = "";
                                    sSql = sSql + " UPDATE  EXT8INSCUR ";
                                    sSql = sSql + "    SET     FKT837CPDALU = " + sCpdAluno;
                                    sSql = sSql + "    FROM    EXT8INSCUR ";
                                    sSql = sSql + "      WHERE   FKT857NUMINS        = " + row["FKT857NUMINS"];
                                    sSql = sSql + "        AND FKT8B7CODIGO    = '" + row["FKT8B7CODIGO"] + "' ";
                                    sSql = sSql + "        AND FKT833CODTUR    = " + row["FKT833CODTUR"];
                                    sSql = sSql + "        AND FKT833ANOREF    = " + row["FKT833ANOREF"];

                                    SqlCommand cmd1 = con.CreateCommand();
                                    try
                                    {
                                        cmd1.CommandText = sSql;
                                        cmd1.Transaction = tranPreIns;
                                        cmd1.ExecuteNonQuery(); // EXECUTA CMD
                                    }
                                    catch (Exception)
                                    {
                                        bRollback = true;
                                    }

                                    if (sSeqReg == null || sAnoReg == null && !bRollback)
                                    {
                                        preparaSqlInsert76();
                                        SqlCommand cmd2 = con.CreateCommand();
                                        try
                                        {
                                            cmd2.CommandText = sSqlInsere76;
                                            cmd2.Transaction = tranPreIns;
                                            cmd2.ExecuteNonQuery(); // EXECUTA CMD
                                            cmd2.Transaction.Commit();
                                            busca76teste();
                                            binseriu76 = true;
                                        }
                                        catch (Exception)
                                        {
                                            bRollback = true;
                                        }

                                        if (binseriu76)
                                        {
                                            busca76();
                                            dataPagTurma = Strings.Format(row["DATPRE"], "dd/MM/yyyy");
                                            if (geraBoletoExtensao())
                                            {
                                                quitaBoletoExtensao();
                                            }
                                            if (bMatExtensao)
                                            {
                                                matriculaAlunoPos(Strings.Left(retornoIns["EX33ANOREF"].ToString(), 4),
                                                                   Strings.Right(retornoIns["EX33ANOREF"].ToString(), 1),
                                                                   retornoIns["EX33CODTUR"].ToString(),
                                                                   Int32.Parse(retornoIns["EX33CTURNO"].ToString()), 1);
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                    if (bRollback)
                    {
                        tranPreIns.Rollback();
                        tranPreIns = null;
                        con.Close();
                        con = null;
                        classCon.fecharConexao();
                        classCon = null;
                        Console.WriteLine("Erro ao Quitar Pre Inscrição NS : " + sNossoNumero);
                    }
                    else
                    {
                        tranPreIns.Commit();
                        con.Close();
                        classCon.fecharConexao();
                        classCon = null;
                        tranPreIns = null;
                    }
                }
            }
        }

        public void removebolsa()
        {
            string buscar = "";
            buscar = " SELECT * FROM PFB9BOLSAT" +
                     " WHERE 1 = 1" +
                     " AND PFB9CPDALU = " + sCpdAluno +
                     " AND PFB9ANOREF = 20202 " +
                     " AND PFB9STATUS = 'A' ";
            if (bCeuma)
            {
                buscar = buscar + " AND FKB9B8CODMOT <> '86'";
            }
            else if (bFamaz)
            {
                buscar = buscar + " AND FKB9B8CODMOT <> '43'";
            }


            cConn.abrirConexao(sStringConexao);
            SqlDataReader retornoVal = cConn.buscaDados(buscar);

            if (retornoVal.HasRows)
            {

                string sqlBolsaUp;
                sqlBolsaUp = " UPDATE PFB9BOLSAT" +
                             " SET PFB9STATUS = 'C' " +
                             " WHERE 1 = 1 " +
                             " AND PFB9CPDALU = " + sCpdAluno +
                             " AND PFB9ANOREF = 20202 " +
                             " AND PFB9STATUS = 'A' ";
                 if (bCeuma)
                {
                    sqlBolsaUp = sqlBolsaUp + " AND FKB9B8CODMOT <> '86'";
                }
                else if (bFamaz)
                {
                    sqlBolsaUp = sqlBolsaUp + " AND FKB9B8CODMOT <> '43'";
                }

                ConnInsert classCon = new ConnInsert();
                SqlConnection con = classCon.abrirConexao(sStringConexao);
                SqlTransaction tranBolsa = con.BeginTransaction();

                SqlCommand cmd1 = con.CreateCommand();
                try
                {
                    cmd1.CommandText = sqlBolsaUp;
                    cmd1.Transaction = tranBolsa;
                    cmd1.ExecuteNonQuery(); // EXECUTA CMD
                }
                catch (Exception)
                {
                    tranBolsa.Rollback();
                    classCon.fecharConexao();
                    cConn.fecharConexao();
                }
                finally
                {
                    tranBolsa.Commit();
                    classCon.fecharConexao();
                    cConn.fecharConexao();
                }
            }
            else
            {
                cConn.fecharConexao();
            }
        }
    }
}
 


