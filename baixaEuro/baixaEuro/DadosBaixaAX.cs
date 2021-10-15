using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace baixaEuro
{
    class DadosBaixaAX
    {
        private string resposta;
        private string[] caminho;
        private string[] linhas;
        private string[] percorrer;
        private int contador;
        private int i;
        private string date;
        private string ano;
        private string dia;
        private string mes;
        private string estacao;

        public string sEstacao { get { return estacao; } set { estacao = value; } }
        public string sAno { get { return ano; } set { ano = value; } }
        public string sDia { get { return dia; } set { dia = value; } }
        public string sMes { get { return mes; } set { mes = value; } }
        public string day { get { return ano; } set { ano = value; } }
        public string sDate { get { return date; } set { date = value; } }
        public int id { get { return i; } set { i = value; } }
        public int iContador { get { return contador; } set { contador = value; } }
        public string sResposta { get { return resposta; } set { resposta = value; } }
        public string[] sCaminho { get { return caminho; } set { caminho = value; } }
        public string[] sLinhas { get { return linhas; } set { linhas = value; } }
        public string[] sPercorrer { get { return percorrer; } set { percorrer = value; } }
        public string sPasta { get { return sourceFile; } set { sourceFile = value; } }

        private string sourceFile;
        private int count;
        private string valorLiq;
        private int valorTotalLiq;
        private string cnpj;
        private string label;
        private string destinationFile;
        private string conexao;
        private string sqlSource;

        public string sSourceFile { get { return sourceFile; } set { sourceFile = value; } }
        public int iCount { get { return count; } set { count = value; } }
        public string sValorLiq { get { return valorLiq; } set { valorLiq = value; } }
        public int sValorTotalLiq { get { return valorTotalLiq; } set { valorTotalLiq = value; } }
        public string sCnpj { get { return cnpj; } set { cnpj = value; } }
        public string sLabel { get { return label; } set { label = value; } }
        public string sDestinationFile { get { return destinationFile; } set { destinationFile = value; } }
        public string sConexao { get { return conexao; } set { conexao = value; } }
        public string sSqlSource { get { return sqlSource; } set { sqlSource = value; } }

        private bool inseriuHeader = false;
        private bool famaz = false;
        private bool euro = false;
        private bool cesup = false;
        private bool ceupi = false;
        private bool ceuma = false;
        private bool lerArquivo = false;
        private bool inseriuMovimento = false;
        private bool inseriuTrailler = false;

        public bool bFamaz { get { return famaz; } set { famaz = value; } }
        public bool bEuro { get { return euro; } set { euro = value; } }
        public bool bCesup { get { return cesup; } set { cesup = value; } }
        public bool bCeupi { get { return ceupi; } set { ceupi = value; } }
        public bool bCeuma { get { return ceuma; } set { ceuma = value; } }

        public bool bInseriuHeader { get { return inseriuHeader; } set { inseriuHeader = value; } }
        public bool bLerArquivos { get { return lerArquivo; } set { lerArquivo = value; } }
        public bool bInseriuMovimento { get { return inseriuMovimento; } set { inseriuMovimento = value; } }
        public bool bInseriuTrailler { get { return inseriuTrailler; } set { inseriuTrailler = value; } }

        //metodo inicial para popular class
        public void VerificarArquivoExistente()
        {
            while (true)
            {
                bLerArquivos = false;
                id = 0;
                Thread.Sleep(5000);
                BuscarCaminho();
                sPercorrer = Directory.GetFiles(sCaminho[0], sCaminho[1]);

                foreach (string aux in sPercorrer)
                {
                    sEstacao = sPercorrer[id].Substring(33, 4);
                    recuperaData(aux);
                    defIESLeitura(sEstacao);
                    lerArquivos(aux);
                    id++;
                }
            }
        }

        public void defIESLeitura(string Estacao)
        {
            switch (Estacao)
            {
                case "OVHF":
                    sLabel = "UNIEURO";
                    sSqlSource = "10.6.0.8";
                    break;

                case "OT9D":
                    sLabel = "UNICEUMA";
                    sSqlSource = "CL3DB";
                    break;

                case "OU7S":
                    sLabel = "IMPERATRIZ";
                    sSqlSource = "CL3DB";
                    break;

                case "OU4N":
                    sLabel = "FAMAZ";
                    sSqlSource = "GENESIS";
                    break;

                case "O78F":
                    sLabel = "CEUPI";
                    sSqlSource = "REIA";
                    break;

                case "1WZ9":
                    sLabel = "CESUP";
                    sSqlSource = "SAGA";
                    break;

            }
            //string de conexão -- tirar user ceuma @@ 
            sConexao = "Data Source=" + sSqlSource + ";Initial Catalog=BDSPFP;User ID=User_GrpCeuma;Password=G*pC&#m@08.19?";
        }

        public void BuscarCaminho()
        {
            string aux = "";
            string buscar = "";

            buscar = " SELECT PFD0ITECOM + '#' + PFD0COMPLE + '#' + PFD0ITEDES AS CAMINHO " +
                     " FROM PFD0CONSTT WHERE PFD0TABELA  = 'CCISA'" +
                     " AND PFD0OBSERV = 'VALIDO'";
            sConexao = "Data Source=CL3DB;Initial Catalog=BDSPFP;User ID=User_GrpCeuma;Password=G*pC&#m@08.19?";
            SqlConnection conexao = new SqlConnection(sConexao);
            conexao.Open();
            SqlCommand cmd = new SqlCommand(buscar, conexao);

            try //Tenta executar o que estiver abaixo
            {

                cmd.ExecuteNonQuery();
                SqlDataReader leitor = cmd.ExecuteReader();

                if (leitor.HasRows)
                {
                    while (leitor.Read())
                    {
                        aux = leitor["CAMINHO"].ToString();
                    }
                }
                leitor.Close();
                leitor.Dispose();
            }
            catch (Exception ex)
            {
                aux = "Erro:" + "#" + ex.Message;
                throw;
            }
            finally
            {
                conexao.Close();
                sCaminho = aux.Substring(0).Split('#');
            }
        }

        public void recuperaData(string aux)
        {
            sAno = "20" + aux.Substring(4, 2);
            sDia = aux.Substring(0, 2);
            sMes = aux.Substring(2, 2);
            DateTime data = new DateTime(
            Int32.Parse(sAno),
            Int32.Parse(sMes),
            Int32.Parse(sDia));
            sDate = data.ToString("dd/MM/yyyy");

            //Recupera o dia da Semana.
            CultureInfo culture = new CultureInfo("pt-BR");
            DateTimeFormatInfo dtfor = culture.DateTimeFormat;
            sDia = dtfor.GetDayName(data.DayOfWeek);
            sPasta = sDate + "-" + sDia;

        }
        public bool verificaCreduc(string linha)
        {
            try
            {
                linha.ToString().Substring(242, 1);
                return false;
            }
            catch
            {
                return true;
            }
        }

        public void lerArquivos(string aux)
        {
            string ID_AX = "0";
            string conta = "";
            int digInicial = 36;
            linhas = System.IO.File.ReadAllLines(aux);
            if (verificaCreduc(linhas[1]))
            {

                sDestinationFile = "C:\\Santander\\AFTData\\INBOX\\GE101300\\CREDUC-" + aux.ToString().Substring(digInicial);
                File.Move(aux, sDestinationFile);

                return;
            }
            MyCisa CisaLeituraArq = new MyCisa();
            foreach (string linha in sLinhas)
            {
                if (linha.ToString().Substring(0, 1) == "9")//verifica se já inseriu todo movimento...
                {
                    bInseriuMovimento = true;
                }

                // verificando se linha é igual a Head do Arquivo 
                if (linha.ToString().Substring(0, 9) == "02RETORNO" && !bInseriuHeader)
                { // se o cabeçalho não tiver inserido

                    if (!MyCisa.ArquivoJaGravado(sConexao, linha.ToString().Substring(94, 6), sLinhas.Length.ToString()))
                    { // caso nao esteja gravado ele vai iniciar a gravacao de leitura do arquivo 

                        // ======== GRAVAR CABEÇALHO ============== 
                        Console.WriteLine("\t" + linha);
                        MyConnection.Conexao(sConexao);
                        MyConnection.TransacaoBegin();
                        CisaLeituraArq.GravarHeader(linha.ToString(), sLinhas.Length.ToString(), ref conta, ref ID_AX);
                        bInseriuHeader = true;
                        MyConnection.TransacaoCommit();
                        MyConnection.fechaConexao();
                    }
                    else
                    { // caso o arquivo ja tenha sido lido vai remover pra pasta de lidos 
                        sDestinationFile = "C:\\ArqLidos\\Duplicados\\";

                        if (!System.IO.File.Exists(sDestinationFile + aux.ToString().Substring(digInicial)))/////////////////////////
                        {
                            sDestinationFile = sDestinationFile + aux.ToString().Substring(digInicial);////////////////////////////
                            File.Move(aux, sDestinationFile);
                        }
                        break;
                    }
                }
                else if (linha.ToString().Substring(0, 1) != "9" && bInseriuHeader && !bInseriuMovimento && ID_AX != "0")
                {// inseriu cabeçalho mas ainda não inseriu todo movimento 
                    sCnpj = linha.ToString().Substring(3, 14);
                    if (linha.ToString().Substring(108, 2) == "06")
                    {
                        iCount = iCount + 1;
                        sValorLiq = linha.Substring(253, 13);
                        sValorTotalLiq = sValorTotalLiq + (Int32.Parse(sValorLiq));
                    }
                    //CisaLeituraArq.GravarHeader(line.ToString(), lines.Length.ToString(), ref ID_AX);
                    MyConnection.Conexao(sConexao);
                    MyConnection.TransacaoBegin();
                    CisaLeituraArq.GravarMovimento(linha.ToString(), ref ID_AX);
                    MyConnection.TransacaoCommit();
                    MyConnection.fechaConexao();
                }

                else if (bInseriuHeader && bInseriuMovimento && !bInseriuTrailler && ID_AX != "0")
                {   //inserindo o Trailler - ANDERSON-ALVES

                    MyConnection.Conexao(sConexao);
                    MyConnection.TransacaoBegin();
                    CisaLeituraArq.GravarTrailler(iCount, sValorTotalLiq, sCnpj, ref ID_AX);
                    MyConnection.TransacaoCommit();
                    bInseriuTrailler = true;
                    MyConnection.fechaConexao();

                    //Movendo Arquivo para outra pasta - ANDERSON-ALVES
                    sPasta = sPasta.Replace("/", "-");
                    sDestinationFile = "C:\\ArqLidos\\Lidos\\" + sLabel + "\\" + sPasta;



                    if (!Directory.Exists(sDestinationFile))
                    {
                        Directory.CreateDirectory(sDestinationFile);
                        sDestinationFile = sDestinationFile + "\\" + aux.ToString().Substring(digInicial);
                        File.Move(aux, sDestinationFile);

                    }
                    else
                    {
                        sDestinationFile = sDestinationFile + "\\" + aux.ToString().Substring(digInicial);

                        if (Directory.Exists(sDestinationFile))
                        {
                            sDestinationFile = "C:\\ArqLidos\\Duplicados";
                            File.Move(aux, sDestinationFile);
                        }
                        else
                        {
                            File.Move(aux, sDestinationFile);
                        }

                    }

                    //Caso tudo esteja OK, Realiza baixa do Boleto.... Anderson Passos
                    if (bInseriuHeader && bInseriuMovimento && bInseriuTrailler && ID_AX != "0")
                    {
                        if (sSqlSource == "CL3DB")
                        {
                            BaixaBoleto baixa = new BaixaBoleto();
                            baixa.baixaAlt(sSqlSource, sConexao, conta, sLabel, ref ID_AX);
                        }

                    }
                }
            }
        }
    }
}
