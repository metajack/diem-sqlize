// test code that round trips a move struct
fn dummy() {
    // testing
    // 0x1::DiemAccount::DiemAccount
    let test_struct = MoveValue::Struct(
        MoveStruct::new(vec![
            // authentication_key
            MoveValue::Vector(vec![MoveValue::U8(0); 32]),
            // withdraw_capability
            MoveValue::Struct(
                // 0x1::Option::Option
                MoveStruct::new(vec![
                    // vec
                    MoveValue::Vector(vec![
                        // 0x1::DiemAccount::WithdrawCapability
                        MoveValue::Struct(
                            MoveStruct::new(vec![
                                // account_address
                                MoveValue::Address(AccountAddress::random()),
                            ]),
                        ),
                    ]),
                ])
            ),
            // key_rotation_capability
            MoveValue::Struct(
                // 0x1::Option::Option
                MoveStruct::new(vec![
                    // vec
                    MoveValue::Vector(vec![
                        // 0x1::DiemAccount::KeyRotationCapability
                        MoveValue::Struct(
                            MoveStruct::new(vec![
                                // account_address
                                MoveValue::Address(AccountAddress::random()),
                            ]),
                        ),
                    ]),
                ])
            ),
            // received_events
            MoveValue::Struct(
                // 0x1::Event::EventHandle
                MoveStruct::new(vec![
                    // counter
                    MoveValue::U64(0),
                    // guid
                    MoveValue::Vector(vec![MoveValue::U8(0); 10]),
                ])
            ),
            // sent_events
            MoveValue::Struct(
                // 0x1::Event::EventHandle
                MoveStruct::new(vec![
                    // counter
                    MoveValue::U64(0),
                    // guid
                    MoveValue::Vector(vec![MoveValue::U8(0); 10]),
                ])
            ),
            // sequence_number
            MoveValue::U64(0),
        ])
    );
    let test_bytes = bcs::to_bytes(&test_struct).unwrap();
    let test_tag = StructTag {
        address: AccountAddress::from_hex_literal("0x1").unwrap(),
        module: Identifier::new("DiemAccount").unwrap(),
        name: Identifier::new("DiemAccount").unwrap(),
        type_params: vec![],
    };
                    let test_resource = annotator.view_resource(&test_tag, &test_bytes).unwrap();
                    println!("{}", test_resource);
    todo!();
}