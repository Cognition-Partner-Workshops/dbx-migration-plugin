#!/bin/ksh
#*************************************************************************/
#* Script Name   : bdx_transfer                                          *
#* Creation Date : 06/14/2019                                            *
#* Description   : SFTP pull of broker bordereaux, per-broker creds.     *
#*                 Adapted from GSS cdc_transfer (same skeleton).        *
#* Called By     : cron (see orchestration/cron/crontab_prod.txt)        *
#*************************************************************************/
p_mailid='[mailbox-redacted]'   # fixture value redacted in this copy
LANDING=/interface/inbound/bordereaux
for BRK in BRK0007 BRK0012 BRK0023 BRK0031; do
  /usr/bin/sftp albion_bdx@sftp.$BRK.example.co.uk <<SFTPEOF
cd outbound
mget CLAIMS_BDX_${BRK}_*.csv $LANDING/
quit
SFTPEOF
done
ls -1 $LANDING | mailx -s "BDX files landed `date '+%d/%m/%Y'`" $p_mailid
