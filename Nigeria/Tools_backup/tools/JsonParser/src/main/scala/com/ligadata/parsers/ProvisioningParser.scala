package com.ligadata.parsers

import com.ligadata.dataobject.{Feed, ProvisioningFeed}
import com.ligadata.dataobject.ProvisioningCls
import org.json4s.{DefaultFormats, Formats}
import org.json4s.jackson.JsonMethods.parse

object ProvisioningParser extends Parser {
  private implicit val jsonFormats: Formats = DefaultFormats
  override def parseJson(jsonString: String): Feed = {

    try {
      val provisioningLog = parse(jsonString).extract[ProvisioningCls.ProvisioningLog]
      val provisioningObj = getProvisioningObj(provisioningLog)
      provisioningObj
    } catch {
      case ex: Exception => {
        throw new Exception(ex.getMessage)
      }
    }
  }

  private def getProvisioningObj(provisioning: ProvisioningCls.ProvisioningLog): ProvisioningFeed = {

    val provisioningObj = new ProvisioningFeed

    provisioningObj.HEADER_TYPE = provisioning.Header.Type
    provisioningObj.HEADER_VER = provisioning.Header.Ver
    provisioningObj.INITIATING_USER_ID = provisioning.Header.InitiatingUser.Id
    provisioningObj.INITIATING_USER_TYPE = provisioning.Header.InitiatingUser.Type
    provisioningObj.REAL_USER_ID = provisioning.Header.RealUser.Id
    provisioningObj.REAL_USER_TYPE = provisioning.Header.RealUser.Type
    provisioningObj.LOGGING_TIME = provisioning.Header.LoggingTime
    provisioningObj.SID = provisioning.Header.Sid
    provisioningObj.TID = provisioning.Header.Tid
    provisioningObj.TRANSACTION_TYPE = provisioning.Header.TransactionType
    provisioningObj.INSTRUCTION_IDENTITIES = if(provisioning.Instruction.Identities.isDefined) provisioning.Instruction.Identities.get.mkString("~") else ""
    provisioningObj.INSTRUCTION_EUI_ID = ConversionHelper.getDefaultLong(provisioning.Instruction.EuiId)
    provisioningObj.INSTRUCTION_REGISTRATION_DATE = ConversionHelper.getDefaultString(provisioning.Instruction.RegistrationDate)
    provisioningObj.INSTRUCTION_IDENTITY_ID = if(provisioning.Instruction.Identity.isDefined) provisioning.Instruction.Identity.get.Id else ""
    provisioningObj.INSTRUCTION_IDENTITY_TYPE = if(provisioning.Instruction.Identity.isDefined) provisioning.Instruction.Identity.get.Type else ""
    provisioningObj.INSTRUCTION_ACTIVATION_DATE = ConversionHelper.getDefaultString(provisioning.Instruction.ActivationDate)
    provisioningObj.INSTRUCTION_LAST_UPDATED_TIMESTAMP = ConversionHelper.getDefaultString(provisioning.Instruction.LastUpdatedTimestamp)
    provisioningObj.INSTRUCTION_STATUS = ConversionHelper.getDefaultString(provisioning.Instruction.Status)
    provisioningObj.INSTRUCTION_SANCTION_STATUS = ConversionHelper.getDefaultString(provisioning.Instruction.SanctionStatus)
    provisioningObj.INSTRUCTION_PROFILE_NAME = ConversionHelper.getDefaultString(provisioning.Instruction.ProfileName)
    provisioningObj.INSTRUCTION_BANK_DOMAIN = ConversionHelper.getDefaultString(provisioning.Instruction.BankDomain)
    provisioningObj.INSTRUCTION_HOME_CHARGING_REGION = ConversionHelper.getDefaultString(provisioning.Instruction.HomeChargingRegion)
    provisioningObj.INSTRUCTION_PROVIDER_CATEGORY = ConversionHelper.getDefaultString(provisioning.Instruction.ProviderCategory)
    provisioningObj.INSTRUCTION_DEFAULT_NOTIFICATION_MSISDN = ConversionHelper.getDefaultString(provisioning.Instruction.DefaultNotificationMSISDN)
    provisioningObj.INSTRUCTION_HIERARCHY_PARENT_ID = if(provisioning.Instruction.Hierarchy.isDefined && provisioning.Instruction.Hierarchy.get.Parent.isDefined) provisioning.Instruction.Hierarchy.get.Parent.get.Id else ""
    provisioningObj.INSTRUCTION_HIERARCHY_PARENT_TYPE = if(provisioning.Instruction.Hierarchy.isDefined && provisioning.Instruction.Hierarchy.get.Parent.isDefined) provisioning.Instruction.Hierarchy.get.Parent.get.Type else ""
    provisioningObj.INSTRUCTION_HIERARCHY_EMPLOYEEID = if(provisioning.Instruction.Hierarchy.isDefined) ConversionHelper.getDefaultString(provisioning.Instruction.Hierarchy.get.EmployeeId) else ""
    provisioningObj.INSTRUCTION_RECRUITED_ID = if(provisioning.Instruction.Recruiter.isDefined) provisioning.Instruction.Recruiter.get.Id else ""
    provisioningObj.INSTRUCTION_RECRUITED_TYPE = if(provisioning.Instruction.Recruiter.isDefined) provisioning.Instruction.Recruiter.get.Type else ""
    provisioningObj.INSTRUCTION_RECRUITED_DATE = ConversionHelper.getDefaultString(provisioning.Instruction.RecruitedDate)
    provisioningObj.INSTRUCTION_PERSONAL_INFO_TITLE = if(provisioning.Instruction.PersonalInfo.isDefined) ConversionHelper.getDefaultString(provisioning.Instruction.PersonalInfo.get.Title) else ""
    provisioningObj.INSTRUCTION_PERSONAL_INFO_OTHER_TITLE = if(provisioning.Instruction.PersonalInfo.isDefined) ConversionHelper.getDefaultString(provisioning.Instruction.PersonalInfo.get.OtherTitle) else ""
    provisioningObj.INSTRUCTION_PERSONAL_INFO_FIRST_NAME = if(provisioning.Instruction.PersonalInfo.isDefined) ConversionHelper.getDefaultString(provisioning.Instruction.PersonalInfo.get.FirstName) else ""
    provisioningObj.INSTRUCTION_PERSONAL_INFO_SUR_NAME = if(provisioning.Instruction.PersonalInfo.isDefined) ConversionHelper.getDefaultString(provisioning.Instruction.PersonalInfo.get.SurName) else ""
    provisioningObj.INSTRUCTION_PERSONAL_INFO_BIRTH_INFORMATION_DATE = if(provisioning.Instruction.PersonalInfo.isDefined && provisioning.Instruction.PersonalInfo.get.BirthInformation.isDefined) ConversionHelper.getDefaultString(provisioning.Instruction.PersonalInfo.get.BirthInformation.get.Date) else ""
    provisioningObj.INSTRUCTION_PERSONAL_INFO_BIRTH_INFORMATION_COUNTRY = if(provisioning.Instruction.PersonalInfo.isDefined && provisioning.Instruction.PersonalInfo.get.BirthInformation.isDefined) ConversionHelper.getDefaultString(provisioning.Instruction.PersonalInfo.get.BirthInformation.get.Country) else ""
    provisioningObj.INSTRUCTION_PERSONAL_INFO_BIRTH_INFORMATION_PROVINCE = if(provisioning.Instruction.PersonalInfo.isDefined && provisioning.Instruction.PersonalInfo.get.BirthInformation.isDefined) ConversionHelper.getDefaultString(provisioning.Instruction.PersonalInfo.get.BirthInformation.get.Province) else ""
    provisioningObj.INSTRUCTION_PERSONAL_INFO_BIRTH_INFORMATION_CITY = if(provisioning.Instruction.PersonalInfo.isDefined && provisioning.Instruction.PersonalInfo.get.BirthInformation.isDefined) ConversionHelper.getDefaultString(provisioning.Instruction.PersonalInfo.get.BirthInformation.get.City) else ""
    provisioningObj.INSTRUCTION_PERSONAL_INFO_GENDER  = if(provisioning.Instruction.PersonalInfo.isDefined) ConversionHelper.getDefaultString(provisioning.Instruction.PersonalInfo.get.Gender) else ""
    provisioningObj.INSTRUCTION_PERSONAL_INFO_LANGUAGE_CODE  = if(provisioning.Instruction.PersonalInfo.isDefined) ConversionHelper.getDefaultString(provisioning.Instruction.PersonalInfo.get.LanguageCode) else ""
    provisioningObj.INSTRUCTION_IDENTIFICATION_ID_TYPE = if(provisioning.Instruction.Identification.isDefined) provisioning.Instruction.Identification.get.IdType else ""
    provisioningObj.INSTRUCTION_IDENTIFICATION_ISSUER = if(provisioning.Instruction.Identification.isDefined) ConversionHelper.getDefaultString(provisioning.Instruction.Identification.get.Issuer) else ""
    provisioningObj.INSTRUCTION_IDENTIFICATION_ISSUER_DATE = if(provisioning.Instruction.Identification.isDefined) ConversionHelper.getDefaultString(provisioning.Instruction.Identification.get.IssueDate) else ""
    provisioningObj.INSTRUCTION_IDENTIFICATION_EXPIRY_DATE = if(provisioning.Instruction.Identification.isDefined) ConversionHelper.getDefaultString(provisioning.Instruction.Identification.get.ExpiryDate) else ""
    provisioningObj.INSTRUCTION_ADDRESS_ADDRESS_LINE0 = if(provisioning.Instruction.Address.isDefined) ConversionHelper.getDefaultString(provisioning.Instruction.Address.get.AddressLine0) else ""
    provisioningObj.INSTRUCTION_ADDRESS_ADDRESS_LINE1 = if(provisioning.Instruction.Address.isDefined) ConversionHelper.getDefaultString(provisioning.Instruction.Address.get.AddressLine1) else ""
    provisioningObj.INSTRUCTION_ADDRESS_POSTAL_CODE = if(provisioning.Instruction.Address.isDefined) ConversionHelper.getDefaultString(provisioning.Instruction.Address.get.PostalCode) else ""
    provisioningObj.INSTRUCTION_ADDRESS_CITY = if(provisioning.Instruction.Address.isDefined) ConversionHelper.getDefaultString(provisioning.Instruction.Address.get.City) else ""
    provisioningObj.INSTRUCTION_ADDRESS_PROVINCE = if(provisioning.Instruction.Address.isDefined) ConversionHelper.getDefaultString(provisioning.Instruction.Address.get.Province) else ""
    provisioningObj.INSTRUCTION_ADDRESS_COUNTRY = if(provisioning.Instruction.Address.isDefined) ConversionHelper.getDefaultString(provisioning.Instruction.Address.get.Country) else ""
    provisioningObj.INSTRUCTION_ADDRESS_REGISTRATION_ADDRESS = if(provisioning.Instruction.Address.isDefined) provisioning.Instruction.Address.get.RegistrationAddress.get.toString else ""
    provisioningObj.INSTRUCTION_PRIMARY_CONTACT_INFO_EMAIL = if(provisioning.Instruction.PrimaryContactInfo.isDefined) ConversionHelper.getDefaultString(provisioning.Instruction.PrimaryContactInfo.get.Email) else ""
    provisioningObj.INSTRUCTION_PRIMARY_CONTACT_INFO_PHONE = if(provisioning.Instruction.PrimaryContactInfo.isDefined) ConversionHelper.getDefaultString(provisioning.Instruction.PrimaryContactInfo.get.Phone) else ""
    provisioningObj.INSTRUCTION_PRIMARY_CONTACT_INFO_FAX = if(provisioning.Instruction.PrimaryContactInfo.isDefined) ConversionHelper.getDefaultString(provisioning.Instruction.PrimaryContactInfo.get.Fax) else ""
    provisioningObj.INSTRUCTION_PRIMARY_CONTACT_INFO_TELEX = if(provisioning.Instruction.PrimaryContactInfo.isDefined) ConversionHelper.getDefaultString(provisioning.Instruction.PrimaryContactInfo.get.Telex) else ""
    provisioningObj.INSTRUCTION_PRIMARY_CONTACT_INFO_URL = if(provisioning.Instruction.PrimaryContactInfo.isDefined) ConversionHelper.getDefaultString(provisioning.Instruction.PrimaryContactInfo.get.URL) else ""
    provisioningObj.INSTRUCTION_PRIMARY_CONTACT_INFO_MOBILE = if(provisioning.Instruction.PrimaryContactInfo.isDefined) ConversionHelper.getDefaultString(provisioning.Instruction.PrimaryContactInfo.get.Mobile) else ""
    provisioningObj.INSTRUCTION_PERSONAL_INFO_EMPLOYING_COMPANY = if(provisioning.Instruction.PersonalInfo.isDefined) ConversionHelper.getDefaultString(provisioning.Instruction.PersonalInfo.get.EmployingCompany) else ""
    provisioningObj.INSTRUCTION_PERSONAL_INFO_PROFESSION = if(provisioning.Instruction.PersonalInfo.isDefined) ConversionHelper.getDefaultString(provisioning.Instruction.PersonalInfo.get.Profession) else ""
    provisioningObj.INSTRUCTION_CONNECTED_ACCOUNT = if(provisioning.Instruction.ConnectedAccount.isDefined) provisioning.Instruction.ConnectedAccount.get.mkString("~") else ""
    provisioningObj.INSTRUCTION_OWNER_ACCOUNT = if(provisioning.Instruction.OwnAccount.isDefined) provisioning.Instruction.OwnAccount.get.mkString("~") else ""
    provisioningObj.INSTRUCTION_ADDITIONAL_INFO_LIST = if(provisioning.Instruction.AdditionalInfoList.isDefined) provisioning.Instruction.AdditionalInfoList.get.mkString("~") else ""
    provisioningObj.INSTRUCTION_SECONDARY_CONTACT_INFO_EMAIL = if(provisioning.Instruction.SecondaryContactInfo.isDefined) ConversionHelper.getDefaultString(provisioning.Instruction.SecondaryContactInfo.get.Email) else ""
    provisioningObj.INSTRUCTION_SECONDARY_CONTACT_INFO_PHONE = if(provisioning.Instruction.SecondaryContactInfo.isDefined) ConversionHelper.getDefaultString(provisioning.Instruction.SecondaryContactInfo.get.Phone) else ""
    provisioningObj.INSTRUCTION_SECONDARY_CONTACT_INFO_FAX = if(provisioning.Instruction.SecondaryContactInfo.isDefined) ConversionHelper.getDefaultString(provisioning.Instruction.SecondaryContactInfo.get.Fax) else ""
    provisioningObj.INSTRUCTION_SECONDARY_CONTACT_INFO_TELEX = if(provisioning.Instruction.SecondaryContactInfo.isDefined) ConversionHelper.getDefaultString(provisioning.Instruction.SecondaryContactInfo.get.Telex) else ""
    provisioningObj.INSTRUCTION_SECONDARY_CONTACT_INFO_URL = if(provisioning.Instruction.SecondaryContactInfo.isDefined) ConversionHelper.getDefaultString(provisioning.Instruction.SecondaryContactInfo.get.URL) else ""
    provisioningObj.INSTRUCTION_SECONDARY_CONTACT_INFO_MOBILE = if(provisioning.Instruction.SecondaryContactInfo.isDefined) ConversionHelper.getDefaultString(provisioning.Instruction.SecondaryContactInfo.get.Mobile) else ""
    provisioningObj.INSTRUCTION_EXTERNAL_DOCUMENT_LIST = if(provisioning.Instruction.ExternalDocumentList.isDefined) provisioning.Instruction.ExternalDocumentList.get.mkString("~") else ""
    provisioningObj.INSTRUCTION_COMPANY_INFO_COUNTRY = if(provisioning.Instruction.CompanyInfo.isDefined) ConversionHelper.getDefaultString(provisioning.Instruction.CompanyInfo.get.Country) else ""
    provisioningObj.INSTRUCTION_COMPANY_INFO_PROVINCE = if(provisioning.Instruction.CompanyInfo.isDefined) ConversionHelper.getDefaultString(provisioning.Instruction.CompanyInfo.get.Province) else ""
    provisioningObj.INSTRUCTION_COMPANY_INFO_CITY = if(provisioning.Instruction.CompanyInfo.isDefined) ConversionHelper.getDefaultString(provisioning.Instruction.CompanyInfo.get.City) else ""
    provisioningObj.INSTRUCTION_COMPANY_INFO_POSTAL_CODE = if(provisioning.Instruction.CompanyInfo.isDefined) ConversionHelper.getDefaultString(provisioning.Instruction.CompanyInfo.get.PostalCode) else ""
    provisioningObj.INSTRUCTION_CITIZENSHIP_NATIONALITY = if(provisioning.Instruction.Citizenship.isDefined) ConversionHelper.getDefaultString(provisioning.Instruction.Citizenship.get.Nationality) else ""
    provisioningObj.INSTRUCTION_CITIZENSHIP_MINOR_INDICATOR = if(provisioning.Instruction.Citizenship.isDefined) provisioning.Instruction.Citizenship.get.MinorIndicator.get.toString else ""
    provisioningObj.INSTRUCTION_GUARDIAN_INFO_FIRST_NAME = if(provisioning.Instruction.GuardianInfo.isDefined) ConversionHelper.getDefaultString(provisioning.Instruction.GuardianInfo.get.FirstName) else ""
    provisioningObj.INSTRUCTION_GUARDIAN_INFO_SUR_NAME = if(provisioning.Instruction.GuardianInfo.isDefined) ConversionHelper.getDefaultString(provisioning.Instruction.GuardianInfo.get.Surname) else ""
    provisioningObj.INSTRUCTION_GUARDIAN_INFO_ID_NUMBER = if(provisioning.Instruction.GuardianInfo.isDefined) ConversionHelper.getDefaultString(provisioning.Instruction.GuardianInfo.get.IdNumber) else ""
    provisioningObj.INSTRUCTION_GUARDIAN_INFO_PHONE_NUMBER = if(provisioning.Instruction.GuardianInfo.isDefined) ConversionHelper.getDefaultString(provisioning.Instruction.GuardianInfo.get.PhoneNumber) else ""
    provisioningObj.INSTRUCTION_GUARDIAN_INFO_STREET_NAME = if(provisioning.Instruction.GuardianInfo.isDefined) ConversionHelper.getDefaultString(provisioning.Instruction.GuardianInfo.get.StreetName) else ""
    provisioningObj.INSTRUCTION_GUARDIAN_INFO_ADDRESS_LINE1 = if(provisioning.Instruction.GuardianInfo.isDefined) ConversionHelper.getDefaultString(provisioning.Instruction.GuardianInfo.get.AddressLine1) else ""
    provisioningObj.INSTRUCTION_GUARDIAN_INFO_POSTAL_CODE = if(provisioning.Instruction.GuardianInfo.isDefined) ConversionHelper.getDefaultString(provisioning.Instruction.GuardianInfo.get.PostalCode) else ""
    provisioningObj.INSTRUCTION_GUARDIAN_INFO_CITY = if(provisioning.Instruction.GuardianInfo.isDefined) ConversionHelper.getDefaultString(provisioning.Instruction.GuardianInfo.get.City) else ""
    provisioningObj.INSTRUCTION_GUARDIAN_INFO_PROVINCE = if(provisioning.Instruction.GuardianInfo.isDefined) ConversionHelper.getDefaultString(provisioning.Instruction.GuardianInfo.get.Province) else ""
    provisioningObj.INSTRUCTION_GUARDIAN_INFO_COUNTRY = if(provisioning.Instruction.GuardianInfo.isDefined) ConversionHelper.getDefaultString(provisioning.Instruction.GuardianInfo.get.Country) else ""
    provisioningObj.INSTRUCTION_NEXT_OF_KIND_INFO_FIRST_NAME = if(provisioning.Instruction.NextofKinInfo.isDefined) ConversionHelper.getDefaultString(provisioning.Instruction.NextofKinInfo.get.FirstName) else ""
    provisioningObj.INSTRUCTION_NEXT_OF_KIND_INFO_SUR_NAME = if(provisioning.Instruction.NextofKinInfo.isDefined) ConversionHelper.getDefaultString(provisioning.Instruction.NextofKinInfo.get.Surname) else ""
    provisioningObj.INSTRUCTION_NEXT_OF_KIND_INFO_ID_NUMBER = if(provisioning.Instruction.NextofKinInfo.isDefined) ConversionHelper.getDefaultString(provisioning.Instruction.NextofKinInfo.get.IdNumber) else ""
    provisioningObj.INSTRUCTION_NEXT_OF_KIND_INFO_PHONE_NUMBER = if(provisioning.Instruction.NextofKinInfo.isDefined) ConversionHelper.getDefaultString(provisioning.Instruction.NextofKinInfo.get.PhoneNumber) else ""
    provisioningObj.INSTRUCTION_NEXT_OF_KIND_INFO_STREET_NAME = if(provisioning.Instruction.NextofKinInfo.isDefined) ConversionHelper.getDefaultString(provisioning.Instruction.NextofKinInfo.get.StreetName) else ""
    provisioningObj.INSTRUCTION_NEXT_OF_KIND_INFO_ADDRESS_LINE1 = if(provisioning.Instruction.NextofKinInfo.isDefined) ConversionHelper.getDefaultString(provisioning.Instruction.NextofKinInfo.get.AddressLine1) else ""
    provisioningObj.INSTRUCTION_NEXT_OF_KIND_INFO_POSTAL_CODE = if(provisioning.Instruction.NextofKinInfo.isDefined) ConversionHelper.getDefaultString(provisioning.Instruction.NextofKinInfo.get.PostalCode) else ""
    provisioningObj.INSTRUCTION_NEXT_OF_KIND_INFO_CITY = if(provisioning.Instruction.NextofKinInfo.isDefined) ConversionHelper.getDefaultString(provisioning.Instruction.NextofKinInfo.get.City) else ""
    provisioningObj.INSTRUCTION_NEXT_OF_KIND_INFO_PROVINCE = if(provisioning.Instruction.NextofKinInfo.isDefined) ConversionHelper.getDefaultString(provisioning.Instruction.NextofKinInfo.get.Province) else ""
    provisioningObj.INSTRUCTION_NEXT_OF_KIND_INFO_COUNTRY = if(provisioning.Instruction.NextofKinInfo.isDefined) ConversionHelper.getDefaultString(provisioning.Instruction.NextofKinInfo.get.Country) else ""
    provisioningObj.INSTRUCTION_FINANCIAL_INFO_TAX_NUMBER = if(provisioning.Instruction.FinancialInfo.isDefined) ConversionHelper.getDefaultString(provisioning.Instruction.FinancialInfo.get.TaxNumber) else ""
    provisioningObj.INSTRUCTION_FINANCIAL_INFO_TAX_REGION = if(provisioning.Instruction.FinancialInfo.isDefined) ConversionHelper.getDefaultString(provisioning.Instruction.FinancialInfo.get.TaxRegion) else ""
    provisioningObj.INSTRUCTION_FINANCIAL_INFO_TAXATION_COUNTRY = if(provisioning.Instruction.FinancialInfo.isDefined) ConversionHelper.getDefaultString(provisioning.Instruction.FinancialInfo.get.TaxationCountry) else ""
    provisioningObj.INSTRUCTION_FINANCIAL_INFO_CREDIT_SCORE = if(provisioning.Instruction.FinancialInfo.isDefined) ConversionHelper.getDefaultString(provisioning.Instruction.FinancialInfo.get.CreditScore) else ""
    provisioningObj.INSTRUCTION_COMPANY_INFO_COMPANY_NAME = if(provisioning.Instruction.CompanyInfo.isDefined) ConversionHelper.getDefaultString(provisioning.Instruction.CompanyInfo.get.CompanyName) else ""
    provisioningObj.INSTRUCTION_COMPANY_INFO_REGISTRATION_NUMBER = if(provisioning.Instruction.CompanyInfo.isDefined) ConversionHelper.getDefaultString(provisioning.Instruction.CompanyInfo.get.RegistrationNumber) else ""
    provisioningObj.INSTRUCTION_COMPANY_INFO_PHONE_NUMBER = if(provisioning.Instruction.CompanyInfo.isDefined) ConversionHelper.getDefaultString(provisioning.Instruction.CompanyInfo.get.PhoneNumber) else ""
    provisioningObj.INSTRUCTION_COMPANY_INFO_FAX_NUMBER = if(provisioning.Instruction.CompanyInfo.isDefined) ConversionHelper.getDefaultString(provisioning.Instruction.CompanyInfo.get.FaxNumber) else ""
    provisioningObj.INSTRUCTION_COMPANY_INFO_EMAIL = if(provisioning.Instruction.CompanyInfo.isDefined) ConversionHelper.getDefaultString(provisioning.Instruction.CompanyInfo.get.Email) else ""
    provisioningObj.INSTRUCTION_COMPANY_INFO_STREET_NAME = if(provisioning.Instruction.CompanyInfo.isDefined) ConversionHelper.getDefaultString(provisioning.Instruction.CompanyInfo.get.StreetName) else ""
    provisioningObj.INSTRUCTION_COMPANY_INFO_ADDRESS_LINE1 = if(provisioning.Instruction.CompanyInfo.isDefined) ConversionHelper.getDefaultString(provisioning.Instruction.CompanyInfo.get.AddressLine1) else ""
    provisioningObj.INSTRUCTION_HIERARCHY_DEFAULT_DISTRIBUTION = if(provisioning.Instruction.Hierarchy.isDefined) ConversionHelper.getDefaultDouble(provisioning.Instruction.Hierarchy.get.DefaultDistribution) else 0.0
    provisioningObj.INSTRUCTION_HIERARCHY_DISTRIBUTION_FACTORY = if(provisioning.Instruction.Hierarchy.isDefined) ConversionHelper.getDefaultDouble(provisioning.Instruction.Hierarchy.get.DistributionFactor) else 0.0
    provisioningObj



  }

  /*
 def main(args: Array[String]): Unit = {
    val x = ProvisioningParser.parseJson("""{"Header":{"Type":"AHFR","InitiatingUser":{"Id":"10008","Type":"MM"},"LoggingTime":"2016-11-10 20:44:35.147","RealUser":{"Id":"10008","Type":"MM"},"Sid":1500000000100,"Tid":152,"TransactionType":"LinkFinancialResourceInformation","Ver":"1.0"},"Instruction":{"Card":[],"ConnectedAccount":[],"LinkedAccount":[{"Id":"789456123@electricity.PIVOT","Type":"SP"}],"OwnAccount":[{"Id":"1000123","Type":"MM"}],"ID":"10008"}}""")
    x.getFields.foreach(value =>{println(value)})
  }*/
}