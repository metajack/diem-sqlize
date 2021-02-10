-- Here are some example queries you can play with:

-- QUERY: find total # of accounts

SELECT COUNT(*)
FROM __root__x1__DiemAccount__DiemAccount
;

-- OUTPUT:
-- 260445

-- count parent vasps

SELECT COUNT(*)
FROM __root__x1__Roles__RoleId rr
INNER JOIN x1__Roles__RoleId r ON rr.id = r.__id
WHERE
  r.role_id = 5
;
 
-- OUTPUT:
-- 26346

-- QUERY: count child vasps per parent

SELECT lower(quote(rr.address)), pv.num_children, COUNT(cv.__id)
FROM __root__x1__Roles__RoleId rr
INNER JOIN x1__Roles__RoleId r ON rr.id = r.__id
INNER JOIN __root__x1__VASP__ParentVASP rpv ON rr.address = rpv.address
INNER JOIN x1__VASP__ParentVASP pv ON rpv.id == pv.__id
LEFT JOIN x1__VASP__ChildVASP cv ON rr.address = cv.parent_vasp_addr
WHERE
  r.role_id = 5
GROUP BY
  rr.address
ORDER BY
  pv.num_children
;
 
-- OUTPUT:
-- x'5426a79a684b1c7ba1660b2dc0a796c6'|256|256
-- x'7f8684b831279985fce0a2d4a1f52d91'|256|256
-- x'8e136697ce2ce3d16129043939a41ed1'|256|256
-- x'9df72fd404fcb5cf27dfcbecc0136d56'|256|256
-- x'b02903ccc219b46738a0e73cae412d15'|256|256
-- x'b86e9110c0a514fd1b05a4a5dbf3bf9a'|256|256
-- x'e12e99169cccafb051092b199bf9060c'|256|256
-- x'e5dc358263f75838439fadc97c74a468'|256|256
-- x'e9f85f5dc5e30305c86a0a7b0e378978'|256|256
-- x'faf6e71ee5027562831520529b4b11b9'|256|256

-- QUERY: count txs per account

SELECT lower(quote(ra.address)), a.sequence_number
FROM __root__x1__DiemAccount__DiemAccount ra
INNER JOIN x1__DiemAccount__DiemAccount a ON ra.id = a.__id
ORDER BY a.sequence_number
;

-- OUTPUT:
-- x'ce06b01fc6d762796ad156c896611fae'|21109
-- x'0000000000000000000000000b1e55ed'|32891
-- x'084e203f53feec474737f81f0df6af95'|39688
-- x'000000000000000000000000000000dd'|47359
-- x'4c51bcec93aac8afc3488beb449316c1'|55073
-- x'afbef937b218f85d59300d2b05c11dd1'|56499
-- x'3489e851e966d55fbbcc55e962f0035d'|56576
-- x'455fb150ba3a5228618a108e3b8992f0'|190971

-- QUERY: get specific account's balance

SELECT lower(quote(rb.address)), c.value
FROM __root__x1__DiemAccount__Balance__t_x1__XUS__XUS_t rb
INNER JOIN x1__DiemAccount__Balance__t_x1__XUS__XUS_t b ON rb.id = b.__id
INNER JOIN x1__Diem__Diem__t_x1__XUS__XUS_t c ON b.coin = c.__id
WHERE
  rb.address = x'455fb150ba3a5228618a108e3b8992f0'
;

-- OUTPUT:
-- x'455fb150ba3a5228618a108e3b8992f0'|3185290000

-- QUERY: tresurance compliances tx fee balance

SELECT lower(quote(rtf.address)), c.value
FROM __root__x1__TransactionFee__TransactionFee__t_x1__XUS__XUS_t rtf
INNER JOIN x1__TransactionFee__TransactionFee__t_x1__XUS__XUS_t tf ON rtf.id = tf.__id
INNER JOIN x1__Diem__Diem__t_x1__XUS__XUS_t c ON tf.balance = c.__id
WHERE
  rtf.address = x'0000000000000000000000000b1e55ed'
;

-- OUTPUT:
-- x'0000000000000000000000000b1e55ed'|352209227

-- QUERY: list currencies in the system

SElECT slot
FROM x1__RegisteredCurrencies__RegisteredCurrencies__currency_codes__elements
;

-- OUTPUT:
-- XUS
-- XDX

-- QUERY: query total value of each currency

SELECT lower(quote(total_value))
FROM x1__Diem__CurrencyInfo__t_x1__XUS__XUS_t
;

-- OUTPUT:
-- x'00000000000000007fffffffffffffff'
-- this is 9223372036854775807

-- QUERY: query total value stored in all balances

SELECT SUM(c.value)
FROM x1__DiemAccount__Balance__t_x1__XUS__XUS_t b
INNER JOIN x1__Diem__Diem__t_x1__XUS__XUS_t c ON b.coin = c.__id
;

-- OUTPUT:
-- 9223372036502566580

-- QUERY: query total value stored in all preburn balances

SELECT SUM(c.value)
FROM x1__Diem__Preburn__t_x1__XUS__XUS_t pb
INNER JOIN x1__Diem__Diem__t_x1__XUS__XUS_t c ON pb.to_burn = c.__id
;

-- OUTPUT:
-- 0

-- QUERY: validators by system info

SELECT COUNT(dsve.slot)
FROM __root__x1__DiemConfig__DiemConfig__t_x1__DiemSystem__DiemSystem_t rdcds
INNER JOIN x1__DiemConfig__DiemConfig__t_x1__DiemSystem__DiemSystem_t dcds ON rdcds.id = dcds.__id
INNER JOIN x1__DiemSystem__DiemSystem ds ON dcds.payload = ds.__id
LEFT JOIN x1__DiemSystem__DiemSystem__validators__elements dsve ON dsve.parent_id = ds.__id
WHERE                                      
  rdcds.address = x'0000000000000000000000000a550c18'
;

-- OUTPUT:
-- 4

-- QUERY: validators by role

SELECT lower(quote(rr.address))
FROM __root__x1__Roles__RoleId rr
INNER JOIN x1__Roles__RoleId r ON rr.id = r.__id
WHERE r.role_id = 3
;

-- OUTPUT:
-- x'a2719315a51388bc1f1e1c5afa2daaa9'
-- x'1e674e850cb4d116babc6f870da9c258'
-- x'd4c4fb4956d899e55289083f45ac5d84'
-- x'57208e640c623b27c6bba704380825ab'


