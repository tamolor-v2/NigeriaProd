package com.ligadata.dataobject

import com.ligadata.parsers.Constants

class ProvisioningFeed extends Feed {

  var HEADER_TYPE: String = ""
  var HEADER_VER: String = ""
  var INITIATING_USER_ID: String = ""
  var INITIATING_USER_TYPE: String = ""
  var REAL_USER_ID: String = ""
  var REAL_USER_TYPE: String = ""
  var LOGGING_TIME: String = ""
  var SID: Long = _
  var TID: Long = _
  var TRANSACTION_TYPE: String = ""
  var INSTRUCTION_IDENTITIES: String = "" // list
  var INSTRUCTION_EUI_ID: Long = _
  var INSTRUCTION_REGISTRATION_DATE: String = ""
  var INSTRUCTION_IDENTITY_ID: String =""
  var INSTRUCTION_IDENTITY_TYPE: String =""
  var INSTRUCTION_ACTIVATION_DATE: String = ""
  var INSTRUCTION_LAST_UPDATED_TIMESTAMP: String = ""
  var INSTRUCTION_STATUS: String = ""
  var INSTRUCTION_SANCTION_STATUS: String = ""
  var INSTRUCTION_PROFILE_NAME: String = ""
  var INSTRUCTION_BANK_DOMAIN: String = ""
  var INSTRUCTION_HOME_CHARGING_REGION: String = ""
  var INSTRUCTION_PROVIDER_CATEGORY: String = ""
  var INSTRUCTION_DEFAULT_NOTIFICATION_MSISDN: String = ""
  var INSTRUCTION_HIERARCHY_PARENT_ID: String = ""
  var INSTRUCTION_HIERARCHY_PARENT_TYPE: String = ""
  var INSTRUCTION_HIERARCHY_EMPLOYEEID: String = ""
  var INSTRUCTION_HIERARCHY_DISTRIBUTION_FACTORY: Double = _
  var INSTRUCTION_HIERARCHY_DEFAULT_DISTRIBUTION: Double = _
  var INSTRUCTION_RECRUITED_ID: String = ""
  var INSTRUCTION_RECRUITED_TYPE: String = ""
  var INSTRUCTION_RECRUITED_DATE: String = ""
  var INSTRUCTION_PERSONAL_INFO_TITLE: String = ""
  var INSTRUCTION_PERSONAL_INFO_OTHER_TITLE: String = ""
  var INSTRUCTION_PERSONAL_INFO_FIRST_NAME: String = ""
  var INSTRUCTION_PERSONAL_INFO_SUR_NAME: String = ""
  var INSTRUCTION_PERSONAL_INFO_BIRTH_INFORMATION_DATE: String = ""
  var INSTRUCTION_PERSONAL_INFO_BIRTH_INFORMATION_COUNTRY: String = ""
  var INSTRUCTION_PERSONAL_INFO_BIRTH_INFORMATION_PROVINCE: String = ""
  var INSTRUCTION_PERSONAL_INFO_BIRTH_INFORMATION_CITY: String = ""
  var INSTRUCTION_PERSONAL_INFO_GENDER: String = ""
  var INSTRUCTION_PERSONAL_INFO_LANGUAGE_CODE: String = ""
  var INSTRUCTION_PERSONAL_INFO_PROFESSION: String = ""
  var INSTRUCTION_PERSONAL_INFO_EMPLOYING_COMPANY: String = ""
  var INSTRUCTION_IDENTIFICATION_ID_TYPE: String = ""
  var INSTRUCTION_IDENTIFICATION_ISSUER: String = ""
  var INSTRUCTION_IDENTIFICATION_ISSUER_DATE: String = ""
  var INSTRUCTION_IDENTIFICATION_EXPIRY_DATE: String = ""
  var INSTRUCTION_ADDRESS_ADDRESS_LINE0: String = ""
  var INSTRUCTION_ADDRESS_ADDRESS_LINE1: String = ""
  var INSTRUCTION_ADDRESS_POSTAL_CODE: String = ""
  var INSTRUCTION_ADDRESS_CITY: String = ""
  var INSTRUCTION_ADDRESS_PROVINCE: String = ""
  var INSTRUCTION_ADDRESS_COUNTRY: String = ""
  var INSTRUCTION_ADDRESS_REGISTRATION_ADDRESS: String = "" // Boolean
  var INSTRUCTION_PRIMARY_CONTACT_INFO_EMAIL: String = ""
  var INSTRUCTION_PRIMARY_CONTACT_INFO_PHONE: String = ""
  var INSTRUCTION_PRIMARY_CONTACT_INFO_MOBILE: String = ""
  var INSTRUCTION_PRIMARY_CONTACT_INFO_FAX: String = ""
  var INSTRUCTION_PRIMARY_CONTACT_INFO_TELEX: String = ""
  var INSTRUCTION_PRIMARY_CONTACT_INFO_URL: String = ""
  var INSTRUCTION_SECONDARY_CONTACT_INFO_EMAIL: String = ""
  var INSTRUCTION_SECONDARY_CONTACT_INFO_PHONE: String = ""
  var INSTRUCTION_SECONDARY_CONTACT_INFO_MOBILE: String = ""
  var INSTRUCTION_SECONDARY_CONTACT_INFO_FAX: String = ""
  var INSTRUCTION_SECONDARY_CONTACT_INFO_TELEX: String = ""
  var INSTRUCTION_SECONDARY_CONTACT_INFO_URL: String = ""
  var INSTRUCTION_CITIZENSHIP_NATIONALITY: String = ""
  var INSTRUCTION_CITIZENSHIP_MINOR_INDICATOR: String = "" //Boolean
  var INSTRUCTION_GUARDIAN_INFO_FIRST_NAME: String = ""
  var INSTRUCTION_GUARDIAN_INFO_SUR_NAME: String = ""
  var INSTRUCTION_GUARDIAN_INFO_ID_NUMBER: String = ""
  var INSTRUCTION_GUARDIAN_INFO_PHONE_NUMBER: String = ""
  var INSTRUCTION_GUARDIAN_INFO_STREET_NAME: String = ""
  var INSTRUCTION_GUARDIAN_INFO_ADDRESS_LINE1: String = ""
  var INSTRUCTION_GUARDIAN_INFO_POSTAL_CODE: String = ""
  var INSTRUCTION_GUARDIAN_INFO_CITY: String = ""
  var INSTRUCTION_GUARDIAN_INFO_PROVINCE: String = ""
  var INSTRUCTION_GUARDIAN_INFO_COUNTRY: String = ""
  var INSTRUCTION_NEXT_OF_KIND_INFO_FIRST_NAME: String = ""
  var INSTRUCTION_NEXT_OF_KIND_INFO_SUR_NAME: String = ""
  var INSTRUCTION_NEXT_OF_KIND_INFO_ID_NUMBER: String = ""
  var INSTRUCTION_NEXT_OF_KIND_INFO_PHONE_NUMBER: String = ""
  var INSTRUCTION_NEXT_OF_KIND_INFO_STREET_NAME: String = ""
  var INSTRUCTION_NEXT_OF_KIND_INFO_ADDRESS_LINE1: String = ""
  var INSTRUCTION_NEXT_OF_KIND_INFO_POSTAL_CODE: String = ""
  var INSTRUCTION_NEXT_OF_KIND_INFO_CITY: String = ""
  var INSTRUCTION_NEXT_OF_KIND_INFO_PROVINCE: String = ""
  var INSTRUCTION_NEXT_OF_KIND_INFO_COUNTRY: String = ""
  var INSTRUCTION_FINANCIAL_INFO_TAX_NUMBER: String = ""
  var INSTRUCTION_FINANCIAL_INFO_TAX_REGION: String = ""
  var INSTRUCTION_FINANCIAL_INFO_TAXATION_COUNTRY: String = ""
  var INSTRUCTION_FINANCIAL_INFO_CREDIT_SCORE: String = ""
  var INSTRUCTION_COMPANY_INFO_COMPANY_NAME: String = ""
  var INSTRUCTION_COMPANY_INFO_REGISTRATION_NUMBER: String = ""
  var INSTRUCTION_COMPANY_INFO_PHONE_NUMBER: String = ""
  var INSTRUCTION_COMPANY_INFO_FAX_NUMBER: String = ""
  var INSTRUCTION_COMPANY_INFO_EMAIL: String = ""
  var INSTRUCTION_COMPANY_INFO_STREET_NAME: String = ""
  var INSTRUCTION_COMPANY_INFO_ADDRESS_LINE1: String = ""
  var INSTRUCTION_COMPANY_INFO_POSTAL_CODE: String = ""
  var INSTRUCTION_COMPANY_INFO_CITY: String = ""
  var INSTRUCTION_COMPANY_INFO_PROVINCE: String = ""
  var INSTRUCTION_COMPANY_INFO_COUNTRY: String = ""
  var INSTRUCTION_EXTERNAL_DOCUMENT_LIST: String = "" // list
  var INSTRUCTION_ADDITIONAL_INFO_LIST: String = "" // list
  var INSTRUCTION_OWNER_ACCOUNT: String = "" // list
  var INSTRUCTION_CONNECTED_ACCOUNT: String = "" // list

  override def getName: String = {
    Constants.PROVISIONING
  }

  override def toString: String = {
    val sb: StringBuilder = new StringBuilder

    sb.append(HEADER_TYPE).append(Constants.DELIMITER)
      .append(HEADER_VER).append(Constants.DELIMITER)
      .append(INITIATING_USER_ID).append(Constants.DELIMITER)
      .append(INITIATING_USER_TYPE).append(Constants.DELIMITER)
      .append(REAL_USER_ID).append(Constants.DELIMITER)
      .append(REAL_USER_TYPE).append(Constants.DELIMITER)
      .append(LOGGING_TIME).append(Constants.DELIMITER)
      .append(SID).append(Constants.DELIMITER)
      .append(TID).append(Constants.DELIMITER)
      .append(TRANSACTION_TYPE).append(Constants.DELIMITER)
      .append(INSTRUCTION_IDENTITIES).append(Constants.DELIMITER)
      .append(INSTRUCTION_EUI_ID).append(Constants.DELIMITER)
      .append(INSTRUCTION_REGISTRATION_DATE).append(Constants.DELIMITER)
      .append(INSTRUCTION_IDENTITY_ID).append(Constants.DELIMITER)
      .append(INSTRUCTION_IDENTITY_TYPE).append(Constants.DELIMITER)
      .append(INSTRUCTION_ACTIVATION_DATE).append(Constants.DELIMITER)
      .append(INSTRUCTION_LAST_UPDATED_TIMESTAMP).append(Constants.DELIMITER)
      .append(INSTRUCTION_STATUS).append(Constants.DELIMITER)
      .append(INSTRUCTION_SANCTION_STATUS).append(Constants.DELIMITER)
      .append(INSTRUCTION_PROFILE_NAME).append(Constants.DELIMITER)
      .append(INSTRUCTION_BANK_DOMAIN).append(Constants.DELIMITER)
      .append(INSTRUCTION_HOME_CHARGING_REGION).append(Constants.DELIMITER)
      .append(INSTRUCTION_PROVIDER_CATEGORY).append(Constants.DELIMITER)
      .append(INSTRUCTION_DEFAULT_NOTIFICATION_MSISDN).append(Constants.DELIMITER)
      .append(INSTRUCTION_HIERARCHY_PARENT_ID).append(Constants.DELIMITER)
      .append(INSTRUCTION_HIERARCHY_PARENT_TYPE).append(Constants.DELIMITER)
      .append(INSTRUCTION_HIERARCHY_EMPLOYEEID).append(Constants.DELIMITER)
      .append(INSTRUCTION_RECRUITED_ID).append(Constants.DELIMITER)
      .append(INSTRUCTION_RECRUITED_TYPE).append(Constants.DELIMITER)
      .append(INSTRUCTION_RECRUITED_DATE).append(Constants.DELIMITER)
      .append(INSTRUCTION_PERSONAL_INFO_TITLE).append(Constants.DELIMITER)
      .append(INSTRUCTION_PERSONAL_INFO_OTHER_TITLE).append(Constants.DELIMITER)
      .append(INSTRUCTION_PERSONAL_INFO_FIRST_NAME).append(Constants.DELIMITER)
      .append(INSTRUCTION_PERSONAL_INFO_SUR_NAME).append(Constants.DELIMITER)
      .append(INSTRUCTION_PERSONAL_INFO_BIRTH_INFORMATION_DATE).append(Constants.DELIMITER)
      .append(INSTRUCTION_PERSONAL_INFO_BIRTH_INFORMATION_COUNTRY).append(Constants.DELIMITER)
      .append(INSTRUCTION_PERSONAL_INFO_BIRTH_INFORMATION_PROVINCE).append(Constants.DELIMITER)
      .append(INSTRUCTION_PERSONAL_INFO_BIRTH_INFORMATION_CITY).append(Constants.DELIMITER)
      .append(INSTRUCTION_PERSONAL_INFO_GENDER).append(Constants.DELIMITER)
      .append(INSTRUCTION_PERSONAL_INFO_LANGUAGE_CODE).append(Constants.DELIMITER)
      .append(INSTRUCTION_IDENTIFICATION_ID_TYPE).append(Constants.DELIMITER)
      .append(INSTRUCTION_IDENTIFICATION_ISSUER).append(Constants.DELIMITER)
      .append(INSTRUCTION_IDENTIFICATION_ISSUER_DATE).append(Constants.DELIMITER)
      .append(INSTRUCTION_IDENTIFICATION_EXPIRY_DATE).append(Constants.DELIMITER)
      .append(INSTRUCTION_ADDRESS_ADDRESS_LINE0).append(Constants.DELIMITER)
      .append(INSTRUCTION_ADDRESS_ADDRESS_LINE1).append(Constants.DELIMITER)
      .append(INSTRUCTION_ADDRESS_POSTAL_CODE).append(Constants.DELIMITER)
      .append(INSTRUCTION_ADDRESS_CITY).append(Constants.DELIMITER)
      .append(INSTRUCTION_ADDRESS_PROVINCE).append(Constants.DELIMITER)
      .append(INSTRUCTION_ADDRESS_COUNTRY).append(Constants.DELIMITER)
      .append(INSTRUCTION_ADDRESS_REGISTRATION_ADDRESS).append(Constants.DELIMITER)
      .append(INSTRUCTION_PRIMARY_CONTACT_INFO_EMAIL).append(Constants.DELIMITER)
      .append(INSTRUCTION_PRIMARY_CONTACT_INFO_PHONE).append(Constants.DELIMITER)
      .append(INSTRUCTION_PRIMARY_CONTACT_INFO_FAX).append(Constants.DELIMITER)
      .append(INSTRUCTION_PRIMARY_CONTACT_INFO_MOBILE).append(Constants.DELIMITER)
      .append(INSTRUCTION_PRIMARY_CONTACT_INFO_TELEX).append(Constants.DELIMITER)
      .append(INSTRUCTION_PRIMARY_CONTACT_INFO_URL).append(Constants.DELIMITER)
      .append(INSTRUCTION_SECONDARY_CONTACT_INFO_EMAIL).append(Constants.DELIMITER)
      .append(INSTRUCTION_SECONDARY_CONTACT_INFO_PHONE).append(Constants.DELIMITER)
      .append(INSTRUCTION_SECONDARY_CONTACT_INFO_FAX).append(Constants.DELIMITER)
      .append(INSTRUCTION_SECONDARY_CONTACT_INFO_MOBILE).append(Constants.DELIMITER)
      .append(INSTRUCTION_SECONDARY_CONTACT_INFO_TELEX).append(Constants.DELIMITER)
      .append(INSTRUCTION_SECONDARY_CONTACT_INFO_URL).append(Constants.DELIMITER)
      .append(INSTRUCTION_CITIZENSHIP_NATIONALITY).append(Constants.DELIMITER)
      .append(INSTRUCTION_CITIZENSHIP_MINOR_INDICATOR).append(Constants.DELIMITER)
      .append(INSTRUCTION_GUARDIAN_INFO_FIRST_NAME).append(Constants.DELIMITER)
      .append(INSTRUCTION_GUARDIAN_INFO_SUR_NAME).append(Constants.DELIMITER)
      .append(INSTRUCTION_GUARDIAN_INFO_ID_NUMBER).append(Constants.DELIMITER)
      .append(INSTRUCTION_GUARDIAN_INFO_PHONE_NUMBER).append(Constants.DELIMITER)
      .append(INSTRUCTION_GUARDIAN_INFO_STREET_NAME).append(Constants.DELIMITER)
      .append(INSTRUCTION_GUARDIAN_INFO_ADDRESS_LINE1).append(Constants.DELIMITER)
      .append(INSTRUCTION_GUARDIAN_INFO_POSTAL_CODE).append(Constants.DELIMITER)
      .append(INSTRUCTION_GUARDIAN_INFO_CITY).append(Constants.DELIMITER)
      .append(INSTRUCTION_GUARDIAN_INFO_PROVINCE).append(Constants.DELIMITER)
      .append(INSTRUCTION_GUARDIAN_INFO_COUNTRY).append(Constants.DELIMITER)
      .append(INSTRUCTION_NEXT_OF_KIND_INFO_FIRST_NAME).append(Constants.DELIMITER)
      .append(INSTRUCTION_NEXT_OF_KIND_INFO_SUR_NAME).append(Constants.DELIMITER)
      .append(INSTRUCTION_NEXT_OF_KIND_INFO_ID_NUMBER).append(Constants.DELIMITER)
      .append(INSTRUCTION_NEXT_OF_KIND_INFO_PHONE_NUMBER).append(Constants.DELIMITER)
      .append(INSTRUCTION_NEXT_OF_KIND_INFO_STREET_NAME).append(Constants.DELIMITER)
      .append(INSTRUCTION_NEXT_OF_KIND_INFO_ADDRESS_LINE1).append(Constants.DELIMITER)
      .append(INSTRUCTION_NEXT_OF_KIND_INFO_POSTAL_CODE).append(Constants.DELIMITER)
      .append(INSTRUCTION_NEXT_OF_KIND_INFO_CITY).append(Constants.DELIMITER)
      .append(INSTRUCTION_NEXT_OF_KIND_INFO_PROVINCE).append(Constants.DELIMITER)
      .append(INSTRUCTION_NEXT_OF_KIND_INFO_COUNTRY).append(Constants.DELIMITER)
      .append(INSTRUCTION_FINANCIAL_INFO_TAX_NUMBER).append(Constants.DELIMITER)
      .append(INSTRUCTION_FINANCIAL_INFO_TAX_REGION).append(Constants.DELIMITER)
      .append(INSTRUCTION_FINANCIAL_INFO_TAXATION_COUNTRY).append(Constants.DELIMITER)
      .append(INSTRUCTION_FINANCIAL_INFO_CREDIT_SCORE).append(Constants.DELIMITER)
      .append(INSTRUCTION_COMPANY_INFO_COMPANY_NAME).append(Constants.DELIMITER)
      .append(INSTRUCTION_COMPANY_INFO_REGISTRATION_NUMBER).append(Constants.DELIMITER)
      .append(INSTRUCTION_COMPANY_INFO_PHONE_NUMBER).append(Constants.DELIMITER)
      .append(INSTRUCTION_COMPANY_INFO_FAX_NUMBER).append(Constants.DELIMITER)
      .append(INSTRUCTION_COMPANY_INFO_EMAIL).append(Constants.DELIMITER)
      .append(INSTRUCTION_COMPANY_INFO_STREET_NAME).append(Constants.DELIMITER)
      .append(INSTRUCTION_COMPANY_INFO_ADDRESS_LINE1).append(Constants.DELIMITER)
      .append(INSTRUCTION_COMPANY_INFO_POSTAL_CODE).append(Constants.DELIMITER)
      .append(INSTRUCTION_COMPANY_INFO_CITY).append(Constants.DELIMITER)
      .append(INSTRUCTION_COMPANY_INFO_PROVINCE).append(Constants.DELIMITER)
      .append(INSTRUCTION_COMPANY_INFO_COUNTRY).append(Constants.DELIMITER)
      .append(INSTRUCTION_EXTERNAL_DOCUMENT_LIST).append(Constants.DELIMITER)
      .append(INSTRUCTION_ADDITIONAL_INFO_LIST).append(Constants.DELIMITER)
      .append(INSTRUCTION_OWNER_ACCOUNT).append(Constants.DELIMITER)
      .append(INSTRUCTION_CONNECTED_ACCOUNT).append(Constants.DELIMITER)
      .append(INSTRUCTION_PERSONAL_INFO_PROFESSION).append(Constants.DELIMITER)
      .append(INSTRUCTION_PERSONAL_INFO_EMPLOYING_COMPANY).append(Constants.DELIMITER)
      .append(INSTRUCTION_HIERARCHY_DISTRIBUTION_FACTORY).append(Constants.DELIMITER)
      .append(INSTRUCTION_HIERARCHY_DEFAULT_DISTRIBUTION).append(Constants.DELIMITER)
    sb.mkString

  }

  override def getFields: Array[String] = {
    val arr: Array[String] = new Array(108)
    arr(0) = HEADER_TYPE
    arr(1) = HEADER_VER
    arr(2) = INITIATING_USER_ID
    arr(3) = INITIATING_USER_TYPE
    arr(4) = REAL_USER_ID
    arr(5) = REAL_USER_TYPE
    arr(6) = LOGGING_TIME
    arr(7) = SID.toString
    arr(8) = TID.toString
    arr(9) = TRANSACTION_TYPE
    arr(10) = INSTRUCTION_IDENTITIES
    arr(11) = INSTRUCTION_EUI_ID.toString
    arr(12) = INSTRUCTION_REGISTRATION_DATE
    arr(13) = INSTRUCTION_IDENTITY_ID
    arr(14) = INSTRUCTION_IDENTITY_TYPE
    arr(15) = INSTRUCTION_ACTIVATION_DATE
    arr(16) = INSTRUCTION_LAST_UPDATED_TIMESTAMP
    arr(17) = INSTRUCTION_STATUS
    arr(18) = INSTRUCTION_SANCTION_STATUS
    arr(19) = INSTRUCTION_PROFILE_NAME
    arr(20) = INSTRUCTION_BANK_DOMAIN
    arr(21) = INSTRUCTION_HOME_CHARGING_REGION
    arr(22) = INSTRUCTION_PROVIDER_CATEGORY
    arr(23) = INSTRUCTION_DEFAULT_NOTIFICATION_MSISDN
    arr(24) = INSTRUCTION_HIERARCHY_PARENT_ID
    arr(25) = INSTRUCTION_HIERARCHY_PARENT_TYPE
    arr(26) = INSTRUCTION_HIERARCHY_EMPLOYEEID
    arr(27) = INSTRUCTION_RECRUITED_ID
    arr(28) = INSTRUCTION_RECRUITED_TYPE
    arr(29) = INSTRUCTION_RECRUITED_DATE
    arr(30) = INSTRUCTION_PERSONAL_INFO_TITLE
    arr(31) = INSTRUCTION_PERSONAL_INFO_OTHER_TITLE
    arr(32) = INSTRUCTION_PERSONAL_INFO_FIRST_NAME
    arr(33) = INSTRUCTION_PERSONAL_INFO_SUR_NAME
    arr(34) = INSTRUCTION_PERSONAL_INFO_BIRTH_INFORMATION_DATE
    arr(35) = INSTRUCTION_PERSONAL_INFO_BIRTH_INFORMATION_COUNTRY
    arr(36) = INSTRUCTION_PERSONAL_INFO_BIRTH_INFORMATION_PROVINCE
    arr(37) = INSTRUCTION_PERSONAL_INFO_BIRTH_INFORMATION_CITY
    arr(38) = INSTRUCTION_PERSONAL_INFO_GENDER
    arr(39) = INSTRUCTION_PERSONAL_INFO_LANGUAGE_CODE
    arr(40) = INSTRUCTION_IDENTIFICATION_ID_TYPE
    arr(41) = INSTRUCTION_IDENTIFICATION_ISSUER
    arr(42) = INSTRUCTION_IDENTIFICATION_ISSUER_DATE
    arr(43) = INSTRUCTION_IDENTIFICATION_EXPIRY_DATE
    arr(44) = INSTRUCTION_ADDRESS_ADDRESS_LINE0
    arr(45) = INSTRUCTION_ADDRESS_ADDRESS_LINE1
    arr(46) = INSTRUCTION_ADDRESS_POSTAL_CODE
    arr(47) = INSTRUCTION_ADDRESS_CITY
    arr(48) = INSTRUCTION_ADDRESS_PROVINCE
    arr(49) = INSTRUCTION_ADDRESS_COUNTRY
    arr(50) = INSTRUCTION_ADDRESS_REGISTRATION_ADDRESS
    arr(51) = INSTRUCTION_PRIMARY_CONTACT_INFO_EMAIL
    arr(52) = INSTRUCTION_PRIMARY_CONTACT_INFO_PHONE
    arr(53) = INSTRUCTION_PRIMARY_CONTACT_INFO_FAX
    arr(54) = INSTRUCTION_PRIMARY_CONTACT_INFO_TELEX
    arr(55) = INSTRUCTION_PRIMARY_CONTACT_INFO_URL
    arr(56) = INSTRUCTION_PRIMARY_CONTACT_INFO_MOBILE
    arr(57) = INSTRUCTION_PERSONAL_INFO_EMPLOYING_COMPANY
    arr(58) = INSTRUCTION_PERSONAL_INFO_PROFESSION
    arr(59) = INSTRUCTION_CONNECTED_ACCOUNT
    arr(60) = INSTRUCTION_OWNER_ACCOUNT
    arr(61) = INSTRUCTION_ADDITIONAL_INFO_LIST
    arr(62) = INSTRUCTION_SECONDARY_CONTACT_INFO_EMAIL
    arr(63) = INSTRUCTION_SECONDARY_CONTACT_INFO_PHONE
    arr(64) = INSTRUCTION_SECONDARY_CONTACT_INFO_FAX
    arr(65) = INSTRUCTION_SECONDARY_CONTACT_INFO_TELEX
    arr(66) = INSTRUCTION_SECONDARY_CONTACT_INFO_URL
    arr(67) = INSTRUCTION_SECONDARY_CONTACT_INFO_MOBILE
    arr(68) = INSTRUCTION_EXTERNAL_DOCUMENT_LIST
    arr(69) = INSTRUCTION_COMPANY_INFO_COUNTRY
    arr(70) = INSTRUCTION_COMPANY_INFO_PROVINCE
    arr(71) = INSTRUCTION_COMPANY_INFO_CITY
    arr(72) = INSTRUCTION_COMPANY_INFO_POSTAL_CODE
    arr(73) = INSTRUCTION_CITIZENSHIP_NATIONALITY
    arr(74) = INSTRUCTION_CITIZENSHIP_MINOR_INDICATOR
    arr(75) = INSTRUCTION_GUARDIAN_INFO_FIRST_NAME
    arr(76) = INSTRUCTION_GUARDIAN_INFO_SUR_NAME
    arr(77) = INSTRUCTION_GUARDIAN_INFO_ID_NUMBER
    arr(78) = INSTRUCTION_GUARDIAN_INFO_PHONE_NUMBER
    arr(79) = INSTRUCTION_GUARDIAN_INFO_STREET_NAME
    arr(80) = INSTRUCTION_GUARDIAN_INFO_ADDRESS_LINE1
    arr(81) = INSTRUCTION_GUARDIAN_INFO_POSTAL_CODE
    arr(82) = INSTRUCTION_GUARDIAN_INFO_CITY
    arr(83) = INSTRUCTION_GUARDIAN_INFO_PROVINCE
    arr(84) = INSTRUCTION_GUARDIAN_INFO_COUNTRY
    arr(85) = INSTRUCTION_NEXT_OF_KIND_INFO_FIRST_NAME
    arr(86) = INSTRUCTION_NEXT_OF_KIND_INFO_SUR_NAME
    arr(87) = INSTRUCTION_NEXT_OF_KIND_INFO_ID_NUMBER
    arr(88) = INSTRUCTION_NEXT_OF_KIND_INFO_PHONE_NUMBER
    arr(89) = INSTRUCTION_NEXT_OF_KIND_INFO_STREET_NAME
    arr(90) = INSTRUCTION_NEXT_OF_KIND_INFO_ADDRESS_LINE1
    arr(91) = INSTRUCTION_NEXT_OF_KIND_INFO_POSTAL_CODE
    arr(92) = INSTRUCTION_NEXT_OF_KIND_INFO_CITY
    arr(93) = INSTRUCTION_NEXT_OF_KIND_INFO_PROVINCE
    arr(94) = INSTRUCTION_NEXT_OF_KIND_INFO_COUNTRY
    arr(95) = INSTRUCTION_FINANCIAL_INFO_TAX_NUMBER
    arr(96) = INSTRUCTION_FINANCIAL_INFO_TAX_REGION
    arr(97) = INSTRUCTION_FINANCIAL_INFO_TAXATION_COUNTRY
    arr(98) = INSTRUCTION_FINANCIAL_INFO_CREDIT_SCORE
    arr(99) = INSTRUCTION_COMPANY_INFO_COMPANY_NAME
    arr(100) = INSTRUCTION_COMPANY_INFO_REGISTRATION_NUMBER
    arr(101) = INSTRUCTION_COMPANY_INFO_PHONE_NUMBER
    arr(102) = INSTRUCTION_COMPANY_INFO_FAX_NUMBER
    arr(103) = INSTRUCTION_COMPANY_INFO_EMAIL
    arr(104) = INSTRUCTION_COMPANY_INFO_STREET_NAME
    arr(105) = INSTRUCTION_COMPANY_INFO_ADDRESS_LINE1
    arr(106) = INSTRUCTION_HIERARCHY_DISTRIBUTION_FACTORY.toString
    arr(107) = INSTRUCTION_HIERARCHY_DEFAULT_DISTRIBUTION.toString
    arr
  }
}

object ProvisioningCls {

  case class ProvisioningLog(Header: Header, Instruction: Instruction)

  case class Header (Type: String,
                     Ver: String,
                     InitiatingUser: IdentityType,
                     RealUser: IdentityType,
                     LoggingTime: String,
                     Sid: Long,
                     Tid: Long,
                     TransactionType: String)

  case class IdentityType (Id: String, Type: String)

  case class Instruction (Identities: Option[List[IdentityType]] = None,
                          EuiId: Option[Long] = None,
                          RegistrationDate: Option[String] = None,
                          ActivationDate: Option[String] = None,
                          LastUpdatedTimestamp:Option[String]= None,
                          Status: Option[String] = None,
                          SanctionStatus: Option[String] = None,
                          ProfileName: Option[String] = None,
                          BankDomain: Option[String] = None,
                          HomeChargingRegion: Option[String] = None,
                          ProviderCategory: Option[String] = None,
                          DefaultNotificationMSISDN: Option[String] = None,
                          Hierarchy: Option[HierarchyType] = None,
                          Recruiter: Option[IdentityType] = None,
                          RecruitedDate: Option[String],
                          PersonalInfo: Option[PersonalInfoType] = None,
                          Identification: Option[IdentificationType] = None,
                          Address: Option[PostalAddress] = None,
                          PrimaryContactInfo: Option[ContactInfoType] = None,
                          SecondaryContactInfo: Option[ContactInfoType] = None,
                          Citizenship: Option[CitizenshipType] = None,
                          GuardianInfo: Option[RelativeInfoType] = None,
                          NextofKinInfo: Option[RelativeInfoType] = None,
                          FinancialInfo: Option[FinancialInfoType] = None,
                          CompanyInfo: Option[CompanyInfoType] = None,
                          ExternalDocumentList: Option[List[ExternalDocument]] = None,
                          AdditionalInfoList: Option[List[AdditionalInfo]] = None,
                          Identity: Option[IdentityType] = None,
                          OwnAccount: Option[List[FRIType]] = None,
                          ConnectedAccount: Option[List[FRIType]] = None)

  case class HierarchyType (Parent: Option[IdentityType] = None,
                            EmployeeId: Option[String] = None,
                            DistributionFactor: Option[Double] = None,
                            DefaultDistribution: Option[Double] = None)

  case class PersonalInfoType (Title: Option[String] = None,
                               OtherTitle: Option[String] = None,
                               FirstName: Option[String] = None,
                               SurName: Option[String] = None,
                               BirthInformation: Option[BirthInformationType] = None,
                               Gender: Option[String] = None,
                               LanguageCode: Option[String] = None,
                               Profession: Option[String] = None,
                               EmployingCompany : Option[String] = None)

  case class BirthInformationType (Date: Option[String] = None,
                                   Country: Option[String] = None,
                                   Province: Option[String] = None,
                                   City : Option[String] = None)

  case class IdentificationType (IdType: String,
                                 Issuer: Option[String] = None,
                                 IssueDate: Option[String] = None,
                                 ExpiryDate: Option[String] = None)

  case class PostalAddress (AddressLine0: Option[String] = None,
                            AddressLine1: Option[String] = None,
                            PostalCode: Option[String] = None,
                            City: Option[String] = None,
                            Province: Option[String] = None,
                            Country: Option[String] = None,
                            RegistrationAddress: Option[Boolean] = None)

  case class ContactInfoType (Email: Option[String] = None,
                              Phone: Option[String] = None,
                              Mobile: Option[String] = None,
                              Fax: Option[String] = None,
                              Telex: Option[String] = None,
                              URL: Option[String] = None)

  case class CitizenshipType (Nationality: Option[String] =None,
                              MinorIndicator: Option[Boolean] =None)

  case class RelativeInfoType (FirstName: Option[String] = None,
                               Surname: Option[String] = None,
                               IdNumber: Option[String] = None,
                               PhoneNumber: Option[String] = None,
                               StreetName: Option[String] = None,
                               AddressLine1: Option[String] = None,
                               PostalCode: Option[String] = None,
                               City: Option[String] = None,
                               Province: Option[String] = None,
                               Country: Option[String] = None)

  case class FinancialInfoType (TaxNumber: Option[String] = None,
                                TaxRegion: Option[String] = None,
                                TaxationCountry: Option[String] = None,
                                CreditScore: Option[String] = None)

  case class CompanyInfoType (CompanyName: Option[String] = None,
                              RegistrationNumber: Option[String] = None,
                              PhoneNumber: Option[String] = None,
                              FaxNumber: Option[String] = None,
                              Email: Option[String] = None,
                              StreetName: Option[String] = None,
                              AddressLine1: Option[String] = None,
                              PostalCode: Option[String] = None,
                              City: Option[String] = None,
                              Province: Option[String] = None,
                              Country: Option[String] = None)

  case class ExternalDocument (DocumentId: String,
                               URL: String)

  case class AdditionalInfo (Key: String,
                             Value: String)

  case class FRIType (Id: String,
                      Type: String,
                      RefProfileName: Option[String])
}