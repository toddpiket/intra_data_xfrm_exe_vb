Option Explicit On
Imports System.Net
Imports System.Web
Imports System.IO
Imports System.Globalization
Imports System.Text
Imports System.Xml
Imports System.Data.SqlClient
Imports System.Text.RegularExpressions
Imports Microsoft.VisualBasic.FileIO
Imports log4net
Imports log4net.Config


Module intra_data_xfrm_exe
    '-------------------------------------------------------------------------------'
    ' Module: intra_data_xfrm_exe
    '    This program will initiate a Data Transform job by sending a SOAP request to
    '    CentralPoint.
    '
    ' Input:  dataTransferKey - Data Transfer key name associated with the data transfer ID
    '                           known by CentralPoint.  The key name to Data ID association
    '                           can be found in the config file.
    '
    ' Tables:
    '    Name                  DB    Select  Insert  Update  Delete  DDL
    ' -----------------------------------------------------------------
    '
    '
    ' Modification History:
    '  Num       Date        Who               What
    ' ------  ------------   ---------------   ------------------------------------
    ' 020717  07-Feb-2017    T. Piket          Initial Version
    '************************************************************************************************

    'instantiate global application config
    Private appConfig As New mauConfig.MAU.config

    ' Declare a public log class variable for later use.
    Private logger As log4net.ILog = log4net.LogManager.GetLogger(System.Reflection.MethodBase.GetCurrentMethod().DeclaringType)

    ' Variables
    Private DestURL As String = appConfig.item("DestURL")

    'Private Cookies As New CookieContainer
    Private RawServerResponse As String = String.Empty
    Private XMLServerResponse As System.Xml.XmlDocument
    Private Username As String = String.Empty
    Private Password As String = String.Empty
    Private CredCache As New CredentialCache
    Private returnCode As Integer = 0
    Private MaxStatusChecks As Integer = 1
    Private DEBUG As Boolean = False
    Private StatusCheckInterval As Integer = 1000
    Private SoapReqWebSiteID As String = String.Empty
    Private SoapReqDataID As String = String.Empty
    Private SoapReqProcessID As String = String.Empty
    Private SoapReqUserName As String = String.Empty
    Private xmlStatusResult As String = String.Empty
    Private xmlStatusResponse As String = String.Empty
    Private xmlStepsResult As String = String.Empty
    Private xmlStepsResponse As String = String.Empty
    Private chkStatusLost As String = String.Empty
    Private chkStatusRemoveLock As String = String.Empty
    Private chkStatusSuccess As String = String.Empty
    Private chkStatusFail As String = String.Empty
    Private intraConnStr As String = appConfig.getConnectionString(appConfig.item("INTRA_DATABASE_NAME"), appConfig.item("INTRA_DATABASE_HOST"), appConfig.item("INTRA_DATABASE_USER"), appConfig.item("INTRA_DATABASE_PASS"))
    Private intraConn As New SqlConnection

    '***************************************************************************'
    Sub Main()
        ' Variables
        Dim httprequest As HttpWebRequest
        Dim soapRequest As String = String.Empty
        Dim counter As Integer = 0
        Dim DTStatus As String = String.Empty

        ' Start off with a try block to capture any exceptions not trapped more locally.
        Try
            ' Execute the subroutine that initializes values and connections.
            Init()

            ' Build the SOAP request to execute the data transfer.
            soapRequest = GenSOAPExeRequest()
            If String.IsNullOrEmpty(soapRequest) Then
                Environment.Exit(2)
            End If

            ' Post the data transfer execute request
            httprequest = PostSoapRequest(soapRequest)

            ' Pause to give the server some breathing room.
            System.Threading.Thread.Sleep(StatusCheckInterval)

            ' Build the SOAP request to check the status of the data transfer.
            soapRequest = String.Empty
            'soapRequest = GenSOAPStatusRequest()
            soapRequest = GenSOAPStepsRequest()
            If String.IsNullOrEmpty(soapRequest) Then
                Environment.Exit(2)
            End If

            While (counter < MaxStatusChecks)
                ' Post the data transfer status request
                httprequest = PostSoapRequest(soapRequest)

                ' Receive the status request response
                'DTStatus = GetStatusResponse(httprequest)
                DTStatus = GetStepsResponse(httprequest)

                ' The steps result string can get big
                If DTStatus.Contains(chkStatusSuccess) Then
                    logger.Info("Data Transfer completed successfully.")
                    Exit While
                ElseIf DTStatus.Contains(chkStatusFail) Then
                    returnCode = 2
                    logger.Info("Data Transfer failed!")
                    Exit While
                ElseIf DTStatus.Contains(chkStatusLost) Then
                    returnCode = 2
                    logger.Info("Data Transfer status cannot be determined.  Check CentralPoint Process Log.")
                    Exit While
                End If
                counter = counter + 1

                logger.Info("Data Transfer " & Environment.GetCommandLineArgs(1) & " still executing.")
                logger.Info("Next status check in " & StatusCheckInterval & " milliseconds.")
                logger.Info("Status checks remaining: " & MaxStatusChecks - counter)

                ' Pause to give the server some breathing room.
                System.Threading.Thread.Sleep(StatusCheckInterval)
            End While
        Catch ex As Exception
            logger.Error(ex.ToString)
            returnCode = 1
        Finally
            ' close any open connections
        End Try

        Environment.Exit(returnCode)
    End Sub

    '***************************************************************************'
    ' Subroutine to initialize parameters and open connections.
    '***************************************************************************'
    Private Sub Init()
        Dim arguments As String() = Environment.GetCommandLineArgs()

        'load log4net config from the defaultENV config
        log4net.Config.XmlConfigurator.Configure(New IO.FileInfo(appConfig.DefaultConfig.FilePath))
        logger.Info("Loaded mauConfig")
        logger.Info("Loaded log4Net Config")
        logger.Info("Config Dump:" & appConfig.dumpAppSettings)

        ' One command line parameter is required.  This argument MUST be a string
        ' that is associated with a DataID (GUID/uniqueidentifier) in the config file.
        ' This ID is used to tell CentralPoint which Data Transfer you want executed.
        ' Note: there is always 1 element in the arguments array because argument(0)
        ' is always the name of the executing program.
        If arguments.Length < 2 Then
            logger.Info("Data Transfer key name MUST be specified on the command line.")
            logger.Info("The configuration file will contain a list of key names similar to:")
            logger.Info(vbTab & "<add key=""StoreHistoricalReportRefresh"" value=""36919cf8-5cc4-4af3-8de2-02f96a3c3d76"" />")
            Environment.Exit(1)
        End If
        ' Get the Data Transform ID required by CentralPoint so it knows which
        ' Data Transform to initiate.
        SoapReqDataID = appConfig.item(Environment.GetCommandLineArgs()(1))
        If String.IsNullOrEmpty(SoapReqDataID) Then
            logger.Info("Data Transfer key name specified is not valid: " & Environment.GetCommandLineArgs()(1))
            Environment.Exit(1)
        End If

        ' Get the web site ID required by CentralPoint so it knows which web site
        ' it controls to give the Data Transfer request to.
        SoapReqWebSiteID = appConfig.item("SOAPReqWebSiteID")

        ' Get the username to associate with this
        ' Data Transform call when it is executed.
        SoapReqUserName = appConfig.item("SOAPReqUserName")

        ' Get other config items
        MaxStatusChecks = CInt(appConfig.item("MAX_STATUS_CHECKS"))
        StatusCheckInterval = CInt(appConfig.item("STATUS_CHECK_INTERVAL"))
        DEBUG = CBool(appConfig.item("DEBUG"))
        xmlStatusResult = appConfig.item("XMLStatusResult")
        xmlStatusResponse = appConfig.item("XMLStatusResponse")
        xmlStepsResult = appConfig.item("XMLStepsResult")
        xmlStepsResponse = appConfig.item("XMLStepsResponse")
        chkStatusLost = appConfig.item("StatusLost")
        chkStatusRemoveLock = appConfig.item("StatusRemoveLock")
        chkStatusSuccess = appConfig.item("StatusSuccess")
        chkStatusFail = appConfig.item("StatusFail")

        ' Open database connections
        logger.Info("Opening connection to CentralPoint database...")
        intraConn = New SqlConnection(intraConnStr)
        intraConn.Open()
        logger.Info("Successfully connected to database(s).")
    End Sub

    Function GenSOAPStatusRequest() As String
        Dim soapRequest As New Text.StringBuilder

        ' Build the SOAP request we will use later
        soapRequest.AppendLine(" <soap12:Envelope xmlns:xsi=""http://www.w3.org/2001/XMLSchema-instance"" xmlns:xsd=""http://www.w3.org/2001/XMLSchema"" xmlns:soap12=""http://www.w3.org/2003/05/soap-envelope""> ")
        soapRequest.AppendLine("   <soap12:Body> ")
        soapRequest.AppendLine("     <GetProcessStatus xmlns=""http://www.oxcyon.com/""> ")
        soapRequest.AppendLine("       <processId>" & SoapReqProcessID & "</processId> ")
        soapRequest.AppendLine("     </GetProcessStatus> ")
        soapRequest.AppendLine("   </soap12:Body> ")
        soapRequest.AppendLine(" </soap12:Envelope> ")

        If DEBUG Then
            logger.Info("Soap Status Request: " & vbCrLf & vbCrLf & soapRequest.ToString)
        End If

        Return soapRequest.ToString
    End Function

    Function GenSOAPStepsRequest() As String
        Dim soapRequest As New Text.StringBuilder

        ' Build the SOAP request we will use later
        soapRequest.AppendLine(" <soap12:Envelope xmlns:xsi=""http://www.w3.org/2001/XMLSchema-instance"" xmlns:xsd=""http://www.w3.org/2001/XMLSchema"" xmlns:soap12=""http://www.w3.org/2003/05/soap-envelope""> ")
        soapRequest.AppendLine("   <soap12:Body> ")
        soapRequest.AppendLine("     <GetProcessSteps xmlns=""http://www.oxcyon.com/""> ")
        soapRequest.AppendLine("       <processId>" & SoapReqProcessID & "</processId> ")
        soapRequest.AppendLine("     </GetProcessSteps> ")
        soapRequest.AppendLine("   </soap12:Body> ")
        soapRequest.AppendLine(" </soap12:Envelope> ")

        If DEBUG Then
            logger.Info("Soap Steps Request: " & vbCrLf & vbCrLf & soapRequest.ToString)
        End If

        Return soapRequest.ToString
    End Function

    Function GenSOAPExeRequest() As String
        Dim xmlAttrs As String = String.Empty
        Dim soapRequest As New Text.StringBuilder

        ' Get a new unique ID to pass in for the processId.
        Using cmd As New SqlCommand("SELECT CONVERT(varchar(255), NEWID()) AS GUID", intraConn)
            Using rdr As SqlDataReader = cmd.ExecuteReader
                While rdr.Read
                    SoapReqProcessID = rdr("GUID").ToString
                End While
            End Using
        End Using

        ' Not receiving a GUID is unlikely, but a fatal error if it occurs.
        If String.IsNullOrEmpty(SoapReqProcessID) Then
            logger.Error("Unable to obtain a unique identifier (GUID)...aborting!")
            Return String.Empty
        End If

        ' Get the XML attributes for the Data Transfer we are invoking.  We have to
        ' pass this in via the SOAP call.  Why?  Ask Oxcyon.
        Using cmd As New SqlCommand("SELECT TOP 1 Attributes FROM cpsys_DataCurrent WHERE DataId = '" & SoapReqDataID & "'", intraConn)
            Using rdr As SqlDataReader = cmd.ExecuteReader
                While rdr.Read
                    xmlAttrs = rdr("Attributes").ToString
                End While
            End Using
        End Using

        ' Not receiving a GUID is unlikely, but a fatal error if it occurs.
        If String.IsNullOrEmpty(xmlAttrs) Then
            logger.Error("Unable to find attributes for data transform with ID " & SoapReqDataID & " ...aborting!")
            Return String.Empty
        End If

        ' Build the SOAP request we will use later
        soapRequest.AppendLine(" <soap12:Envelope xmlns:xsi=""http://www.w3.org/2001/XMLSchema-instance"" xmlns:xsd=""http://www.w3.org/2001/XMLSchema"" xmlns:soap12=""http://www.w3.org/2003/05/soap-envelope""> ")
        soapRequest.AppendLine("   <soap12:Body> ")
        soapRequest.AppendLine("     <DataTransferExecute xmlns=""http://www.oxcyon.com/""> ")
        soapRequest.AppendLine("       <webSiteId>" & SoapReqWebSiteID & "</webSiteId> ")
        soapRequest.AppendLine("       <dataId>" & SoapReqDataID & "</dataId> ")
        soapRequest.AppendLine("       <attributesXml><![CDATA[" & xmlAttrs & "]]></attributesXml> ")
        soapRequest.AppendLine("       <processId>" & SoapReqProcessID & "</processId> ")
        soapRequest.AppendLine("       <userName>" & SoapReqUserName & "</userName> ")
        soapRequest.AppendLine("     </DataTransferExecute> ")
        soapRequest.AppendLine("   </soap12:Body> ")
        soapRequest.AppendLine(" </soap12:Envelope> ")

        If DEBUG Then
            logger.Info("Soap EXE Request: " & vbCrLf & vbCrLf & soapRequest.ToString)
        End If

        ' Return success
        Return soapRequest.ToString
    End Function

    Function PostSoapRequest(soapRequest As String) As HttpWebRequest
        Dim postData As String
        Dim byteArray As Byte()
        Dim dataStream As Stream
        Dim request As HttpWebRequest

        'Set Header/Meta Info
        request = HttpWebRequest.Create(DestURL)
        request.Method = "POST"
        request.ContentType = "text/xml; charset=""utf-8"""
        request.Accept = "text/xml; charset=""utf-8"""
        request.UserAgent = "Mozilla/5.0 (Windows; U;Windows NT 5.1; en-US; rv:1.8.1.1) Gecko/20061204 Firefox/2.0.0.1"
        request.AllowAutoRedirect = True
        request.Timeout = 420000  ' 7 minutes (420000 milliseconds)

        ' Build the XML SOAP Envelope we are about to Send
        postData = "<?xml version='1.0' encoding='UTF-8'?>" & vbCrLf
        postData = postData & soapRequest
        byteArray = Encoding.UTF8.GetBytes(postData)
        request.ContentLength = byteArray.Length

        'Write to the request stream
        dataStream = request.GetRequestStream()
        dataStream.Write(byteArray, 0, byteArray.Length)
        dataStream.Close()

        Return request
    End Function

    Function GetStatusResponse(request As HttpWebRequest) As String
        Dim reader As StreamReader
        Dim response As HttpWebResponse
        Dim dataStream As Stream
        Dim status As String = String.Empty

        response = request.GetResponse()
        dataStream = response.GetResponseStream()
        reader = New StreamReader(dataStream)
        RawServerResponse = reader.ReadToEnd()
        If DEBUG Then
            logger.Info("Raw Server Response:")
            logger.Info(RawServerResponse.ToString)
        End If
        XMLServerResponse = New System.Xml.XmlDocument
        XMLServerResponse.LoadXml(RawServerResponse)

        For Each n As XmlNode In XMLServerResponse.GetElementsByTagName(xmlStatusResponse)
            For Each n2 As XmlNode In n
                If n2.Name.Equals(xmlStatusResult) Then
                    status = n(xmlStatusResult).InnerText
                End If
            Next
        Next

        ' Clean up the streams.
        reader.Close()
        dataStream.Close()
        response.Close()

        ' Return success
        Return status
    End Function

    Function GetStepsResponse(request As HttpWebRequest) As String
        Dim reader As StreamReader
        Dim response As HttpWebResponse
        Dim dataStream As Stream
        Dim steps As String = String.Empty

        response = request.GetResponse()
        dataStream = response.GetResponseStream()
        reader = New StreamReader(dataStream)
        RawServerResponse = reader.ReadToEnd()
        If DEBUG Then
            logger.Info("Raw Server Response:")
            logger.Info(RawServerResponse.ToString)
        End If
        XMLServerResponse = New System.Xml.XmlDocument
        XMLServerResponse.LoadXml(RawServerResponse)

        For Each n As XmlNode In XMLServerResponse.GetElementsByTagName(xmlStepsResponse)
            For Each n2 As XmlNode In n
                If n2.Name.Equals(xmlStepsResult) Then
                    steps = n(xmlStepsResult).InnerText
                End If
            Next
        Next

        ' Clean up the streams.
        reader.Close()
        dataStream.Close()
        response.Close()

        ' Return success
        Return steps
    End Function
End Module
